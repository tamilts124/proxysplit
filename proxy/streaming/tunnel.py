"""
proxy/streaming/tunnel.py
CONNECT tunnel handler and raw TCP dispatch factory.
Handles CONNECT tunnelling through HTTP and SOCKS proxies,
WebSocket upgrade detection and relay, plus bidirectional
byte-pipe for plain HTTP forwarding.
"""

import asyncio

from proxy.logging_setup import log
from proxy.session_pool import pick_proxy


async def handle_connect(
    host: str, port: int,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
):
    proxy_url = pick_proxy()
    log.debug(f"CONNECT {host}:{port} via {proxy_url}")
    try:
        if proxy_url and proxy_url.startswith("socks"):
            from python_socks.async_.asyncio import Proxy
            sock = await Proxy.from_url(proxy_url).connect(
                dest_host=host, dest_port=port, timeout=15)
            up_r, up_w = await asyncio.open_connection(sock=sock)
        else:
            import urllib.parse, base64
            parsed = urllib.parse.urlparse(proxy_url)
            up_r, up_w = await asyncio.open_connection(
                parsed.hostname, parsed.port or 8080)
            auth = ""
            if parsed.username:
                creds = base64.b64encode(
                    f"{parsed.username}:{parsed.password}".encode()).decode()
                auth = f"Proxy-Authorization: Basic {creds}\r\n"
            up_w.write(
                f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n{auth}\r\n"
                .encode())
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
                try: dst_w.close()
                except Exception: pass

        await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer))

    except Exception as e:
        log.debug(f"CONNECT error {host}:{port}: {e}")
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            await writer.drain()
        except Exception:
            pass
    finally:
        try: writer.close()
        except Exception: pass


def make_raw_handler(internal_port: int):
    """Raw TCP handler: dispatches CONNECT tunnels, WebSocket upgrades,
    and plain HTTP (forwarded to internal aiohttp)."""

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            first_line = await asyncio.wait_for(reader.readline(), timeout=10)
        except Exception:
            try: writer.close()
            except Exception: pass
            return

        # ── CONNECT tunnel ────────────────────────────────────────────────────
        if first_line.upper().startswith(b"CONNECT "):
            parts = first_line.split()
            if len(parts) >= 2:
                hp   = parts[1].decode(errors="ignore")
                host, _, ps = hp.rpartition(":")
                port = int(ps) if ps.isdigit() else 443
                while True:
                    line = await reader.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                await handle_connect(host, port, reader, writer)
            else:
                try: writer.close()
                except Exception: pass
            return

        # ── Non-CONNECT: peek headers, detect WebSocket, then forward ─────────
        # Read up to 4 KB of headers without blocking indefinitely.
        try:
            peek = await asyncio.wait_for(reader.read(4096), timeout=2)
        except (asyncio.TimeoutError, Exception):
            peek = b""

        combined = first_line + peek

        if b"upgrade: websocket" in combined.lower():
            from proxy.streaming.websocket import handle_websocket_upgrade
            # Replay combined bytes through a fresh StreamReader so the
            # WebSocket handler can parse them line-by-line.
            fake_reader = asyncio.StreamReader()
            fake_reader.feed_data(combined)
            fake_reader.feed_eof()
            handled = await handle_websocket_upgrade(first_line, fake_reader, writer)
            if handled:
                return

        # ── Plain HTTP: forward to internal aiohttp ───────────────────────────
        try:
            fwd_r, fwd_w = await asyncio.open_connection("127.0.0.1", internal_port)
            fwd_w.write(combined)
            await fwd_w.drain()

            async def _pr():
                try:
                    while True:
                        d = await reader.read(65536)
                        if not d: break
                        fwd_w.write(d); await fwd_w.drain()
                except Exception: pass
                finally:
                    try: fwd_w.close()
                    except Exception: pass

            async def _ps():
                try:
                    while True:
                        d = await fwd_r.read(65536)
                        if not d: break
                        writer.write(d); await writer.drain()
                except Exception: pass

            await asyncio.gather(_pr(), _ps())
        except Exception:
            pass
        finally:
            try: writer.close()
            except Exception: pass

    return handler
