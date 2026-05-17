"""
proxy/streaming/websocket.py
WebSocket proxy relay.

Detects an Upgrade: websocket request and performs a bidirectional
relay through the selected proxy.

For HTTP proxies: issues CONNECT to the upstream proxy, then completes
the WebSocket handshake over that tunnel.
For SOCKS proxies: connects via the SOCKS connector, then relays.

This extends the CONNECT tunnel already handled by tunnel.py.
Integration: called from tunnel.py's make_raw_handler when the
incoming request contains "Upgrade: websocket".
"""

import asyncio
import base64
import hashlib
import urllib.parse
from typing import Optional

from proxy.logging_setup import log
from proxy.session_pool import pick_proxy


_WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept_key(client_key: str) -> str:
    sha = hashlib.sha1((client_key + _WS_GUID.decode()).encode()).digest()
    return base64.b64encode(sha).decode()


async def _pipe(src_r: asyncio.StreamReader, dst_w: asyncio.StreamWriter, label: str):
    try:
        while True:
            data = await src_r.read(65536)
            if not data:
                break
            dst_w.write(data)
            await dst_w.drain()
    except Exception as exc:
        log.debug(f"WebSocket pipe {label} closed: {exc}")
    finally:
        try:
            dst_w.close()
        except Exception:
            pass


async def handle_websocket_upgrade(
    first_line: bytes,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    pool: Optional[str] = None,
) -> bool:
    """
    Attempt to proxy a WebSocket upgrade request.

    Reads all request headers from reader, connects upstream via the
    chosen proxy, replays the full HTTP Upgrade request, and bridges
    both directions until either side closes.

    Returns True if the upgrade was handled (even if it ultimately failed),
    False if the request is not a WebSocket upgrade (caller should handle it).
    """
    # Read remaining headers
    header_lines = [first_line]
    while True:
        line = await reader.readline()
        header_lines.append(line)
        if line in (b"\r\n", b"\n", b""):
            break

    raw_headers = b"".join(header_lines)
    header_str  = raw_headers.decode("latin-1", errors="replace")

    # Verify this really is a WebSocket upgrade
    has_upgrade   = "upgrade: websocket" in header_str.lower()
    has_connection = "upgrade" in header_str.lower().split("connection:")[-1].split("\r\n")[0].lower() if "connection:" in header_str.lower() else False
    if not has_upgrade:
        return False

    log.info(f"WebSocket upgrade detected")

    # Parse target host from request line
    parts = first_line.decode(errors="replace").split()
    if len(parts) < 2:
        writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        return True

    method, path = parts[0], parts[1]
    # path may be ws://host/... or /path (Host header)
    parsed = urllib.parse.urlparse(path)
    if parsed.scheme in ("ws", "wss"):
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path_only = parsed.path or "/"
        if parsed.query:
            path_only += "?" + parsed.query
    else:
        # Extract Host header
        host = None
        for line in header_str.splitlines():
            if line.lower().startswith("host:"):
                host = line.split(":", 1)[1].strip()
                if ":" in host:
                    host, _, portstr = host.rpartition(":")
                    port = int(portstr)
                else:
                    port = 80
                break
        if not host:
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await writer.drain()
            return True
        path_only = path

    proxy_url = pick_proxy(pool=pool)
    log.info(f"WebSocket relay: {host}:{port} via {proxy_url}")

    try:
        if proxy_url and proxy_url.startswith("socks"):
            from python_socks.async_.asyncio import Proxy
            sock  = await Proxy.from_url(proxy_url).connect(
                dest_host=host, dest_port=port, timeout=15)
            up_r, up_w = await asyncio.open_connection(sock=sock)
        elif proxy_url:
            psd = urllib.parse.urlparse(proxy_url)
            up_r, up_w = await asyncio.open_connection(
                psd.hostname, psd.port or 8080)
            auth = ""
            if psd.username:
                creds = base64.b64encode(
                    f"{psd.username}:{psd.password}".encode()).decode()
                auth = f"Proxy-Authorization: Basic {creds}\r\n"
            up_w.write(
                f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n{auth}\r\n"
                .encode())
            await up_w.drain()
            # Read CONNECT response
            resp_line = await up_r.readline()
            while True:
                line = await up_r.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
            if b"200" not in resp_line:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                return True
        else:
            up_r, up_w = await asyncio.open_connection(host, port)

        # Replay the WebSocket upgrade request to the upstream
        # Rewrite the request line to use relative path
        rebuilt = f"GET {path_only} HTTP/1.1\r\n"
        for line in header_str.splitlines()[1:]:
            if line.lower().startswith("host:"):
                rebuilt += f"Host: {host}:{port}\r\n"
            else:
                rebuilt += line + "\r\n"
        up_w.write(rebuilt.encode("latin-1"))
        await up_w.drain()

        # Bridge both directions
        await asyncio.gather(
            _pipe(reader, up_w, "client→upstream"),
            _pipe(up_r, writer, "upstream→client"),
        )

    except Exception as exc:
        log.warning(f"WebSocket relay error {host}:{port}: {exc}")
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

    return True
