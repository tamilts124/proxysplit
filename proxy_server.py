#!/usr/bin/env python3
"""
Chunk-Level Parallel Proxy Server
==================================
- Splits any HTTP request into byte-range chunks (configurable size)
- Each chunk is fetched via a different random proxy (HTTP or SOCKS5)
- Minimum 2 proxies required — enforced at startup
- All chunks fetched in parallel, streamed in order to the browser
- Proxy rotation: consecutive chunks always use different proxies
- Chunk-level retry: failed chunks are retried with a fresh proxy (up to N attempts)
- Accept-Ranges header respected: falls back to single fetch when server doesn’t support ranges
- Extension/MIME exclusion list: HTML, JSON, text files etc. skip chunking entirely (config)
- --use-free-list flag: downloads fresh proxies from ProxyScrape JSON API;
    if free-proxy-list.txt already exists, prompts to re-download or reuse it
- True streaming to browser: chunks are written to the response as they arrive
- POST body forwarding
- /health and /status endpoints
"""

import argparse
import asyncio
import aiohttp
import random
import logging
import yaml
import os
import sys
import time
from aiohttp import web
from aiohttp_socks import ProxyConnector
from typing import Optional

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("chunk-proxy")

# Startup timestamp for /status uptime
_START_TIME = time.monotonic()

# ── Suppress WinError 10054 noise (Windows Proactor asyncio bug) ─────────────
def _suppress_connection_reset(loop, context):
    ex = context.get("exception")
    if isinstance(ex, (ConnectionResetError, OSError)) and getattr(ex, "winerror", None) == 10054:
        return
    loop.default_exception_handler(context)

# ─── CLI Args ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Chunk-Level Parallel Proxy Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python proxy_server.py
  python proxy_server.py --proxy-file proxies.txt
  python proxy_server.py --proxy-file http.txt --proxy-file socks5.txt
  python proxy_server.py --config custom.yaml --proxy-file extra.txt

proxy file format (one per line, blanks and # comments ignored):
  http://1.2.3.4:8080
  http://user:pass@5.6.7.8:3128
  socks4://9.10.11.12:1080
  socks5://9.10.11.12:1080
  socks5://user:pass@13.14.15.16:1080
        """
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        metavar="FILE",
        help="Path to config YAML file (default: config.yaml)"
    )
    parser.add_argument(
        "--proxy-file", "-f",
        action="append",
        default=[],
        metavar="FILE",
        dest="proxy_files",
        help="Text file with proxy URLs, one per line. Can be specified multiple times."
    )
    parser.add_argument(
        "--use-free-list",
        action="store_true",
        default=False,
        help="Auto-load free-proxy-list.txt from the same directory as the config file."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)"
    )
    return parser.parse_args()

# ─── Config / Proxy Loading ──────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def load_proxy_file(path: str) -> list[str]:
    """Load proxies from a text file — one per line, skip blanks and # comments."""
    proxies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    log.info(f"   Loaded {len(proxies)} proxies from file: {path}")
    return proxies

def collect_proxies(args, config: dict) -> list[str]:
    """Merge proxies from config + CLI files + optional free list (deduped, order-preserved).
    
    When --use-free-list is given:
      - If the cached file does not exist → download from the ProxyScrape JSON API.
      - If the cached file already exists → ask the user whether to re-download or reuse it.
    """
    seen = set()
    proxies = []

    def _add(p: str):
        if p not in seen:
            proxies.append(p)
            seen.add(p)

    for p in config.get("proxies", []):
        _add(p)

    for pfile in args.proxy_files:
        for p in load_proxy_file(pfile):
            _add(p)

    if args.use_free_list:
        config_dir = os.path.dirname(os.path.abspath(args.config))
        free_filename = config.get("free_proxy_file", "free-proxy-list.txt")
        free_list_path = os.path.join(config_dir, free_filename)
        api_url = config.get(
            "free_proxy_api",
            "https://api.proxyscrape.com/v4/free-proxy-list/get"
            "?request=display_proxies&proxy_format=protocolipport&format=json",
        )

        should_download = True
        if os.path.exists(free_list_path):
            # File exists — ask the user
            print(f"\n📋 Free proxy list already exists: {free_list_path}")
            while True:
                choice = input("   Re-download from API? [y/n]: ").strip().lower()
                if choice in ("y", "yes"):
                    should_download = True
                    break
                elif choice in ("n", "no"):
                    should_download = False
                    break
                else:
                    print("   Please enter y or n.")

        if should_download:
            log.info(f"Downloading free proxy list from: {api_url}")
            try:
                import urllib.request, json as _json
                with urllib.request.urlopen(api_url, timeout=30) as resp:
                    raw = resp.read().decode("utf-8")
                data = _json.loads(raw)

                # ProxyScrape v4 JSON format:
                # { "proxies": [ {"proxy": "protocol://ip:port", ...}, ... ] }
                # or a plain list of strings depending on format param.
                downloaded: list[str] = []
                if isinstance(data, dict) and "proxies" in data:
                    for item in data["proxies"]:
                        if isinstance(item, dict):
                            p = item.get("proxy") or item.get("ip") or ""
                            proto = item.get("protocol", "http").lower()
                            port = item.get("port", "")
                            if p and ":" not in p:
                                p = f"{proto}://{p}:{port}"
                        else:
                            p = str(item)
                        if p:
                            downloaded.append(p.strip())
                elif isinstance(data, list):
                    downloaded = [str(x).strip() for x in data if x]
                else:
                    log.warning("Unexpected JSON structure from proxy API; trying raw lines")
                    downloaded = [l.strip() for l in raw.splitlines() if l.strip() and not l.startswith("#")]

                with open(free_list_path, "w") as f:
                    f.write("\n".join(downloaded))
                log.info(f"   Saved {len(downloaded)} proxies to {free_list_path}")
            except Exception as exc:
                log.error(f"Failed to download free proxy list: {exc}")
                if os.path.exists(free_list_path):
                    log.info("   Falling back to existing file")
                else:
                    log.warning("   No local file to fall back to; skipping free list")

        if os.path.exists(free_list_path):
            for p in load_proxy_file(free_list_path):
                _add(p)
        else:
            log.warning(f"--use-free-list: file not found at {free_list_path}")

    return proxies

# ─── Startup Proxy Health Check ──────────────────────────────────────────────

# socks4 and socks4a are both handled by ProxyConnector.from_url() via aiohttp-socks
SUPPORTED_SCHEMES = ("http://", "https://", "socks4://", "socks4a://", "socks5://")

def _filter_supported(proxies: list[str]) -> tuple[list[str], int]:
    """Drop truly unsupported schemes and log what's skipped."""
    supported, skipped = [], 0
    for p in proxies:
        if any(p.startswith(s) for s in SUPPORTED_SCHEMES):
            supported.append(p)
        else:
            log.info(f"   - skip   {p}  (unsupported scheme)")
            skipped += 1
    return supported, skipped

async def _check_proxy_async(session_factory, proxy_url: str, timeout: int = 10) -> bool:
    """
    Async proxy liveness check using aiohttp.
    Also checks that the proxy is anonymous by verifying the returned IP
    differs from our own (best-effort; skipped if own-IP lookup fails).
    """
    test_url = "https://httpbin.org/ip"
    try:
        connector = make_connector(proxy_url)
        proxy_param = http_proxy_param(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                test_url,
                proxy=proxy_param,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
                proxy_seen_ip = data.get("origin", "")
                # Check anonymity: proxy should not reveal our real IP
                own_ip = session_factory.get("own_ip")
                if own_ip and own_ip in proxy_seen_ip:
                    log.info(f"   ~ transparent {proxy_url} (leaks real IP)")
                    return False
                return True
    except Exception:
        return False

async def _get_own_ip(timeout: int = 5) -> Optional[str]:
    """Best-effort: find our own public IP for anonymity comparison."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://httpbin.org/ip",
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                data = await resp.json()
                return data.get("origin")
    except Exception:
        return None

async def filter_live_proxies_async(proxies: list[str]) -> list[str]:
    """Filter to supported + alive (and non-transparent) proxies using asyncio."""
    supported, skipped = _filter_supported(proxies)
    log.info(
        f"Checking {len(supported)} proxies concurrently "
        f"({skipped} skipped — unsupported scheme)..."
    )
    own_ip = await _get_own_ip()
    if own_ip:
        log.info(f"   Own IP: {own_ip} (used for anonymity check)")
    else:
        log.info("   Could not determine own IP; anonymity check skipped")

    # Shared dict so each check can read own_ip without closure weirdness
    session_factory = {"own_ip": own_ip}

    results = await asyncio.gather(
        *[_check_proxy_async(session_factory, p) for p in supported],
        return_exceptions=True,
    )

    live = []
    for p, ok in zip(supported, results):
        if isinstance(ok, Exception):
            ok = False
        status = "✓ alive" if ok else "✗ dead "
        log.info(f"   {status}  {p}")
        if ok:
            live.append(p)

    log.info(f"   {len(live)}/{len(supported)} proxies alive ({skipped} skipped)")
    return live

def validate_proxies(proxies: list[str]):
    if len(proxies) < 2:
        raise SystemExit(
            f"\n❌ ERROR: At least 2 proxies are required.\n"
            f"   Currently loaded: {len(proxies)} proxy/proxies\n"
            f"   Add proxies via config.yaml OR --proxy-file proxies.txt\n"
        )

# ─── Proxy Helpers ───────────────────────────────────────────────────────────

def make_connector(proxy_url: Optional[str]) -> aiohttp.BaseConnector:
    """Return an aiohttp connector suitable for the proxy type.
    
    aiohttp-socks handles socks4://, socks4a://, and socks5:// via ProxyConnector.
    HTTP proxies are passed as a request-level parameter instead.
    """
    if proxy_url and proxy_url.startswith("socks"):
        return ProxyConnector.from_url(proxy_url)
    return aiohttp.TCPConnector()

def http_proxy_param(proxy_url: Optional[str]) -> Optional[str]:
    """Return proxy URL only for HTTP proxies (aiohttp uses request-level param)."""
    if proxy_url and proxy_url.startswith("http"):
        return proxy_url
    return None

def random_proxy(proxies: list[str], exclude: Optional[str] = None) -> str:
    """
    Pick a random proxy from the pool.
    If 'exclude' is given and pool has >1 proxy, avoids returning the same proxy.
    """
    if exclude and len(proxies) > 1:
        choices = [p for p in proxies if p != exclude]
        return random.choice(choices)
    return random.choice(proxies)

# ─── Session Pool ────────────────────────────────────────────────────────────
# Re-use ClientSession per proxy URL across chunks in one server lifetime.
# Each session owns its connector, so SOCKS sessions are keyed separately.

class SessionPool:
    def __init__(self):
        self._sessions: dict[str, aiohttp.ClientSession] = {}

    def get(self, proxy_url: Optional[str]) -> aiohttp.ClientSession:
        key = proxy_url or "__direct__"
        if key not in self._sessions or self._sessions[key].closed:
            connector = make_connector(proxy_url)
            self._sessions[key] = aiohttp.ClientSession(connector=connector)
        return self._sessions[key]

    async def close_all(self):
        for session in self._sessions.values():
            if not session.closed:
                await session.close()
        self._sessions.clear()

# Global pool — created in main(), injected into app
_SESSION_POOL: Optional[SessionPool] = None

# ─── Proxy Distributor ───────────────────────────────────────────────────────

def distribute_proxies(proxies: list[str], num_chunks: int) -> list[str]:
    """
    Assign a proxy to each chunk such that:
    - No two consecutive chunks share the same proxy
    - Distribution is random (shuffled each call)
    Uses a post-pass to fix any remaining consecutive duplicates.
    """
    if len(proxies) == 1:
        return [proxies[0]] * num_chunks

    pool = proxies.copy()
    random.shuffle(pool)

    assigned: list[str] = []
    for i in range(num_chunks):
        candidate = pool[i % len(pool)]
        # Fix consecutive duplicate with a proper exclusion loop
        if assigned and candidate == assigned[-1]:
            for alt in pool:
                if alt != assigned[-1]:
                    candidate = alt
                    break
        assigned.append(candidate)
    return assigned

# ─── Chunk Fetcher ───────────────────────────────────────────────────────────

async def fetch_chunk(
    url: str,
    start: int,
    end: int,
    index: int,
    proxy_url: str,
    proxies: list[str],
    chunk_retries: int,
) -> tuple[int, bytes, int]:
    """
    Fetch a single byte-range chunk through the assigned proxy.
    Retries up to chunk_retries times on failure, each time with a fresh proxy.
    Returns (index, data, http_status).
    """
    headers = {"Range": f"bytes={start}-{end}"}
    last_exc: Exception = RuntimeError("No attempts made")
    current_proxy = proxy_url

    for attempt in range(1, chunk_retries + 1):
        log.debug(
            f"  Chunk #{index:03d} bytes {start:,}-{end:,} "
            f"via {current_proxy} (attempt {attempt}/{chunk_retries})"
        )
        try:
            session = _SESSION_POOL.get(current_proxy)
            proxy_param = http_proxy_param(current_proxy)
            async with session.get(
                url,
                headers=headers,
                proxy=proxy_param,
                timeout=aiohttp.ClientTimeout(total=60, connect=10),
            ) as resp:
                data = await resp.read()
                return index, data, resp.status
        except Exception as exc:
            last_exc = exc
            log.warning(
                f"  Chunk #{index:03d} attempt {attempt} failed via {current_proxy}: {exc}"
            )
            if attempt < chunk_retries:
                current_proxy = random_proxy(proxies, exclude=current_proxy)

    raise last_exc

# ─── HEAD Discovery ──────────────────────────────────────────────────────────

async def get_head_info(
    url: str,
    req_headers: dict,
    proxies: list[str],
    head_retries: int = 3,
) -> tuple[Optional[int], bool, str]:
    """
    HEAD request to discover Content-Length, Accept-Ranges, Content-Type.
    Retries with different proxies on failure (fix #2: no SPOF on HEAD).
    """
    forward = {k: v for k, v in req_headers.items()
               if k.lower() in ("accept", "user-agent", "accept-language")}

    last_exc: Exception = RuntimeError("No attempts")
    tried: set[str] = set()

    for attempt in range(1, head_retries + 1):
        proxy_url = random_proxy(proxies, exclude=next(iter(tried), None))
        tried.add(proxy_url)
        try:
            session = _SESSION_POOL.get(proxy_url)
            proxy_param = http_proxy_param(proxy_url)
            async with session.head(
                url,
                headers=forward,
                proxy=proxy_param,
                timeout=aiohttp.ClientTimeout(total=15, connect=8),
                allow_redirects=True,
            ) as resp:
                cl = resp.headers.get("Content-Length")
                accepts_ranges = resp.headers.get("Accept-Ranges", "none").lower()
                content_type = resp.headers.get("Content-Type", "application/octet-stream")
                supports_ranges = accepts_ranges == "bytes"
                log.info(
                    f"HEAD {url} → {resp.status}, "
                    f"Content-Length={cl}, "
                    f"Accept-Ranges={accepts_ranges}, "
                    f"Content-Type={content_type}"
                )
                return (int(cl) if cl else None), supports_ranges, content_type
        except Exception as exc:
            last_exc = exc
            log.warning(f"HEAD attempt {attempt} failed via {proxy_url}: {exc}")

    raise last_exc

# ─── Fallback: Single fetch ──────────────────────────────────────────────────

async def fetch_whole(
    url: str,
    req_headers: dict,
    proxies: list[str],
    body: Optional[bytes] = None,
    method: str = "GET",
) -> tuple[int, bytes, dict]:
    """Fallback when server doesn't support range requests. Supports POST body."""
    proxy_url = random_proxy(proxies)
    session = _SESSION_POOL.get(proxy_url)
    proxy_param = http_proxy_param(proxy_url)
    forward = {k: v for k, v in req_headers.items()
               if k.lower() in ("accept", "user-agent", "accept-language", "accept-encoding", "content-type")}

    log.info(f"Fallback: single {method} fetch via {proxy_url}")
    async with session.request(
        method,
        url,
        headers=forward,
        proxy=proxy_param,
        data=body,
        timeout=aiohttp.ClientTimeout(total=120, connect=10),
    ) as resp:
        data = await resp.read()
        return resp.status, data, dict(resp.headers)

# ─── CONNECT Tunnel Handler ──────────────────────────────────────────────────

async def handle_connect(
    host: str, port: int,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    proxies: list[str],
):
    """Tunnel a CONNECT request through a random proxy."""
    proxy_url = random_proxy(proxies)
    log.debug(f"CONNECT {host}:{port} via {proxy_url}")

    try:
        if proxy_url.startswith("socks"):
            # python_socks handles socks4, socks4a, and socks5
            from python_socks.async_.asyncio import Proxy
            proxy = Proxy.from_url(proxy_url)
            sock = await proxy.connect(dest_host=host, dest_port=port, timeout=15)
            up_r, up_w = await asyncio.open_connection(sock=sock)
        else:
            import urllib.parse, base64
            parsed = urllib.parse.urlparse(proxy_url)
            up_r, up_w = await asyncio.open_connection(parsed.hostname, parsed.port or 8080)
            auth = ""
            if parsed.username:
                creds = base64.b64encode(
                    f"{parsed.username}:{parsed.password}".encode()
                ).decode()
                auth = f"Proxy-Authorization: Basic {creds}\r\n"
            up_w.write(
                f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n{auth}\r\n".encode()
            )
            await up_w.drain()
            resp_line = await up_r.readline()
            while True:
                line = await up_r.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
            if b"200" not in resp_line:
                writer.close()
                return

        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()

        async def pipe(src_r, dst_w):
            try:
                while True:
                    data = await src_r.read(65536)
                    if not data:
                        break
                    dst_w.write(data)
                    await dst_w.drain()
            except Exception:
                pass
            finally:
                try:
                    dst_w.close()
                except Exception:
                    pass

        await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer))

    except Exception as e:
        log.debug(f"CONNECT tunnel error {host}:{port}: {e}")
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await writer.drain()
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def make_raw_handler(proxies: list[str], internal_port: int):
    async def raw_connection_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """
        Low-level TCP handler that peeks at the first line:
          - CONNECT → tunnel via handle_connect()
          - anything else → forward to internal aiohttp port
        """
        try:
            first_line = await asyncio.wait_for(reader.readline(), timeout=10)
        except Exception:
            writer.close()
            return

        if first_line.upper().startswith(b"CONNECT "):
            parts = first_line.split()
            if len(parts) >= 2:
                hostport = parts[1].decode(errors="ignore")
                host, _, port_s = hostport.rpartition(":")
                port = int(port_s) if port_s.isdigit() else 443
                while True:
                    line = await reader.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                await handle_connect(host, port, reader, writer, proxies)
            else:
                writer.close()
        else:
            try:
                fwd_r, fwd_w = await asyncio.open_connection("127.0.0.1", internal_port)
                fwd_w.write(first_line)
                await fwd_w.drain()

                async def _pipe_request():
                    try:
                        while True:
                            chunk = await reader.read(65536)
                            if not chunk:
                                break
                            fwd_w.write(chunk)
                            await fwd_w.drain()
                    except Exception:
                        pass
                    finally:
                        try:
                            fwd_w.close()
                        except Exception:
                            pass

                async def _pipe_response():
                    try:
                        while True:
                            chunk = await fwd_r.read(65536)
                            if not chunk:
                                break
                            writer.write(chunk)
                            await writer.drain()
                    except Exception:
                        pass

                await asyncio.gather(_pipe_request(), _pipe_response())
            except Exception:
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

    return raw_connection_handler

# ─── Health / Status Endpoints ───────────────────────────────────────────────

def make_health_handler(proxies: list[str]):
    async def handle_health(request: web.Request) -> web.Response:
        if len(proxies) >= 2:
            return web.Response(text="ok", content_type="text/plain")
        return web.Response(status=503, text="insufficient proxies", content_type="text/plain")
    return handle_health

def make_status_handler(proxies: list[str], config: dict):
    async def handle_status(request: web.Request) -> web.Response:
        uptime = int(time.monotonic() - _START_TIME)
        body = {
            "uptime_seconds": uptime,
            "proxy_count": len(proxies),
            "proxies": proxies,
            "chunk_size": config.get("chunk_size", 512 * 1024),
            "max_workers": config.get("max_workers", 10),
            "chunk_retries": config.get("chunk_retries", 2),
        }
        import json
        return web.Response(
            text=json.dumps(body, indent=2),
            content_type="application/json",
        )
    return handle_status

# ─── Exclusion helpers ───────────────────────────────────────────────────────

def _is_excluded(url: str, content_type: str, config: dict) -> bool:
    """
    Return True if this URL/content-type should skip chunked fetching
    and fall back to a single request.

    Checks:
      1. URL path extension against config['no_chunk_extensions']
      2. Content-Type header against config['no_chunk_mimetypes'] prefixes
    """
    from urllib.parse import urlparse
    import posixpath

    path = urlparse(url).path.lower()
    ext = posixpath.splitext(path)[1]   # e.g. ".html"

    excluded_exts = config.get("no_chunk_extensions", [])
    if ext and ext in excluded_exts:
        log.info(f"    Extension {ext!r} is in no_chunk_extensions — using single fetch")
        return True

    ct_lower = content_type.lower()
    excluded_mimes = config.get("no_chunk_mimetypes", [])
    for mime_prefix in excluded_mimes:
        if ct_lower.startswith(mime_prefix.lower()):
            log.info(f"    Content-Type {content_type!r} matches no_chunk_mimetypes ({mime_prefix!r}) — using single fetch")
            return True

    return False


# ─── Main Proxy Handler ───────────────────────────────────────────────────────

def make_request_handler(proxies: list[str], config: dict):
    chunk_size    = config.get("chunk_size", 512 * 1024)
    max_workers   = config.get("max_workers", 10)
    chunk_retries = config.get("chunk_retries", 2)

    async def handle_request(request: web.Request) -> web.StreamResponse | web.Response:
        url = request.rel_url.query.get("url")
        if not url:
            return web.Response(
                status=400,
                content_type="text/plain",
                text=(
                    "Chunk Proxy: missing ?url= parameter.\n"
                    "Usage: GET http://localhost:8888/proxy?url=https://example.com/file\n"
                ),
            )

        if not url.startswith(("http://", "https://")):
            return web.Response(status=400, text=f"Unsupported scheme in URL: {url}")

        log.info(f"─── New request → {url} [{request.method}]")
        log.info(
            f"    Chunk size: {chunk_size:,} bytes | "
            f"Workers: {max_workers} | Retries/chunk: {chunk_retries}"
        )

        req_headers = dict(request.headers)

        # Read POST/PUT body if present
        body: Optional[bytes] = None
        if request.method in ("POST", "PUT", "PATCH"):
            body = await request.read()

        try:
            result = await _handle_inner(
                url, req_headers, body, request.method,
                proxies, chunk_size, max_workers, chunk_retries, config,
            )
            return result
        except _StreamReady as sr:
            # ── True streaming path: write chunks to the browser as they arrive
            resp = sr.response
            await resp.prepare(request)
            bytes_sent = 0
            for i in range(sr.num_chunks):
                chunk_data = sr.chunks[i]
                await resp.write(chunk_data)
                bytes_sent += len(chunk_data)
            await resp.write_eof()
            log.info(f"    Streamed {bytes_sent:,} bytes in {sr.num_chunks} chunks ✓")
            return resp
        except Exception as exc:
            import traceback
            log.error(f"Unhandled error for {url}:\n{traceback.format_exc()}")
            return web.Response(status=502, content_type="text/plain", text=f"Proxy error: {exc}")

    return handle_request


async def _handle_inner(
    url: str,
    req_headers: dict,
    body: Optional[bytes],
    method: str,
    proxies: list[str],
    chunk_size: int,
    max_workers: int,
    chunk_retries: int,
    config: dict,
) -> web.StreamResponse | web.Response:

    # ── Step 1: HEAD to discover content-length, range support, content-type
    content_length: Optional[int] = None
    supports_ranges: bool = False
    content_type: str = "application/octet-stream"
    try:
        content_length, supports_ranges, content_type = await get_head_info(
            url, req_headers, proxies
        )
    except Exception as e:
        log.warning(f"HEAD failed ({e}), falling back to single fetch")

    # ── Step 2: Check exclusion list by URL extension or content-type
    if _is_excluded(url, content_type, config):
        reason = "excluded by config (extension or MIME type)"
        log.info(f"    Fallback reason: {reason}")
        status, data, headers = await fetch_whole(url, req_headers, proxies, body, method)
        ct = headers.get("Content-Type", content_type)
        return web.Response(status=status, body=data, content_type=ct.split(";")[0].strip())

    # ── Step 3: Fall back if no content-length or server rejects ranges
    if content_length is None or not supports_ranges:
        reason = "no Content-Length" if content_length is None else "Accept-Ranges not supported"
        log.info(f"    Fallback reason: {reason}")
        status, data, headers = await fetch_whole(url, req_headers, proxies, body, method)
        ct = headers.get("Content-Type", content_type)
        return web.Response(status=status, body=data, content_type=ct.split(";")[0].strip())

    # ── Step 4: Build chunk ranges
    ranges = []
    start = 0
    idx = 0
    while start < content_length:
        end = min(start + chunk_size - 1, content_length - 1)
        ranges.append((start, end, idx))
        start = end + 1
        idx += 1

    num_chunks = len(ranges)
    assigned_proxies = distribute_proxies(proxies, num_chunks)

    log.info(
        f"    Total size: {content_length:,} bytes → "
        f"{num_chunks} chunks across {len(set(assigned_proxies))} unique proxies"
    )

    # ── Step 5: Parallel fetch bounded by MAX_WORKERS
    semaphore = asyncio.Semaphore(max_workers)

    async def bounded_fetch(s, e, i, proxy_url):
        async with semaphore:
            return await fetch_chunk(url, s, e, i, proxy_url, proxies, chunk_retries)

    tasks = [bounded_fetch(s, e, i, assigned_proxies[i]) for s, e, i in ranges]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # ── Step 6: Collect results and check for errors
    chunks: dict[int, bytes] = {}
    for result in results:
        if isinstance(result, Exception):
            log.error(f"Chunk fetch error: {result}")
            return web.Response(status=502, text=f"Chunk fetch failed: {result}")
        index, data, status = result
        chunks[index] = data

    # ── Step 7: Stream chunks to the browser in order
    use_streaming = config.get("streaming", True)
    ct_clean = content_type.split(";")[0].strip()

    if use_streaming:
        response = web.StreamResponse(status=200)
        response.content_type = ct_clean
        response.content_length = content_length
        # Allow the browser to start rendering / the download manager to show progress
        response.headers["Accept-Ranges"] = "bytes"
        response.headers["X-Proxy-Chunks"] = str(num_chunks)

        # We need a Request object to call prepare(); we receive it via closure
        # (injected by make_request_handler below)
        raise _StreamReady(response, chunks, num_chunks, content_length)

    # ── Step 8 (non-streaming): Reassemble and verify
    assembled = b"".join(chunks[i] for i in range(len(ranges)))
    if len(assembled) != content_length:
        log.error(
            f"Length mismatch: expected {content_length:,} bytes, "
            f"got {len(assembled):,} bytes"
        )
        return web.Response(
            status=502,
            text=(
                f"Reassembly error: expected {content_length} bytes, "
                f"got {len(assembled)} bytes"
            ),
        )
    log.info(f"    Reassembled {len(assembled):,} bytes from {len(ranges)} chunks ✓")
    return web.Response(status=200, body=assembled, content_type=ct_clean)


class _StreamReady(Exception):
    """Sentinel to carry streaming state back up to the request handler."""
    def __init__(self, response, chunks, num_chunks, content_length):
        self.response = response
        self.chunks = chunks
        self.num_chunks = num_chunks
        self.content_length = content_length

# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    logging.getLogger().setLevel(args.log_level)
    logging.getLogger("chunk-proxy").setLevel(args.log_level)

    log.info(f"Config  : {args.config}")
    if args.proxy_files:
        log.info(f"Proxy files: {', '.join(args.proxy_files)}")
    if args.use_free_list:
        log.info("Free list : enabled")

    config = load_config(args.config)
    proxies = collect_proxies(args, config)

    chunk_size    = config.get("chunk_size", 512 * 1024)
    max_workers   = config.get("max_workers", 10)
    listen_host   = config.get("host", "0.0.0.0")
    listen_port   = config.get("port", 8888)
    chunk_retries = config.get("chunk_retries", 2)

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(_suppress_connection_reset)
    asyncio.set_event_loop(loop)

    log.info(f"Loaded {len(proxies)} proxies total, running health check...")
    proxies = loop.run_until_complete(filter_live_proxies_async(proxies))

    validate_proxies(proxies)
    log.info(f"✓  {len(proxies)} proxies ready")
    for i, p in enumerate(proxies):
        log.info(f"   [{i+1}] {p}")

    global _SESSION_POOL
    _SESSION_POOL = SessionPool()

    app = web.Application()
    handler = make_request_handler(proxies, config)
    app.router.add_route("GET",    "/proxy",  handler)
    app.router.add_route("POST",   "/proxy",  handler)
    app.router.add_route("PUT",    "/proxy",  handler)
    app.router.add_route("PATCH",  "/proxy",  handler)
    app.router.add_route("DELETE", "/proxy",  handler)
    app.router.add_get("/health", make_health_handler(proxies))
    app.router.add_get("/status", make_status_handler(proxies, config))

    log.info(f"🚀 Chunk Proxy listening on {listen_host}:{listen_port} (CONNECT + HTTP)")
    log.info(f"   Chunk size : {chunk_size:,} bytes")
    log.info(f"   Max workers: {max_workers}")

    import signal

    def _shutdown(sig, frame):
        log.info("Shutting down...")
        loop.call_soon_threadsafe(loop.stop)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    class _FilteredAccessLogger(web.AccessLogger):
        def log(self, request, response, time):
            if request.method in ("UNKNOWN", "CONNECT"):
                return
            super().log(request, response, time)

    async def _start():
        runner = web.AppRunner(app, access_log_class=_FilteredAccessLogger)
        await runner.setup()

        internal_site = web.TCPSite(runner, "127.0.0.1", 0)
        await internal_site.start()
        internal_port = internal_site._server.sockets[0].getsockname()[1]
        log.info(f"   Internal aiohttp on 127.0.0.1:{internal_port}")

        raw_handler = make_raw_handler(proxies, internal_port)
        server = await asyncio.start_server(raw_handler, listen_host, listen_port)

        async with server:
            try:
                await loop.create_future()
            except asyncio.CancelledError:
                pass

        await runner.cleanup()
        await _SESSION_POOL.close_all()

    try:
        loop.run_until_complete(_start())
    except RuntimeError:
        pass


if __name__ == "__main__":
    main()
