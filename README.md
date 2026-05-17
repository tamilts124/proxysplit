# proxy-chunk-speedup

**Parallel chunk proxy that splits any HTTP download across multiple proxies simultaneously.**

Each request is broken into byte-range chunks, every chunk is fetched through a different proxy in parallel, and the assembled result is streamed directly to your browser or client — no buffering wait. Text-based types (HTML, JSON, JS, CSS, etc.) are detected automatically and sent via a fast single-fetch path instead.

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
- **True browser streaming** — chunks are piped to the client as they arrive (low TTFB, visible progress)
- **Smart fallback** — HTML, JSON, XML, JS, CSS, SVG and other text types skip chunking and use a direct single fetch; fully configurable via `no_chunk_extensions` / `no_chunk_mimetypes`
- **Free proxy integration** — downloads a fresh list from the ProxyScrape JSON API; if a cached list already exists, prompts whether to re-download or reuse it
- **Startup health + anonymity check** — dead and transparent (IP-leaking) proxies are filtered out before the server starts
- **SOCKS4 / SOCKS5 / HTTP proxy support** — with optional username:password auth
- **CONNECT tunnel support** — HTTPS traffic tunnelled correctly through HTTP proxies
- **Chunk-level retry** — failed chunks are retried with a fresh proxy, up to a configurable limit
- **POST / PUT / PATCH body forwarding**
- **`/health` and `/status` monitoring endpoints**

---

## Install

```bash
pip install -r requirements.txt
```

---

## Quick Start

```bash
# 1. Copy the example config
cp config.example.yaml config.yaml

# 2. Add your proxies to config.yaml  (or use --use-free-list below)

# 3. Start the server
python proxy_server.py
```

Then send requests with the target URL as a query parameter:

```bash
# Download a large file through the chunk proxy
curl "http://localhost:8888/proxy?url=https://example.com/largefile.zip" -o output.zip

# Simple page fetch
curl "http://localhost:8888/proxy?url=https://httpbin.org/get"

# POST with body forwarding
curl -X POST "http://localhost:8888/proxy?url=https://httpbin.org/post" \
     -H "Content-Type: application/json" \
     -d '{"key": "value"}'
```

---

## Configuration

`config.yaml` (copy from `config.example.yaml`):

```yaml
host: "0.0.0.0"
port: 8888

chunk_size: 524288   # 512 KB per chunk (tune up for fast proxies, down for many small ones)
max_workers: 10      # max parallel chunk fetches in flight at once
chunk_retries: 2     # retry attempts per failed chunk (fresh proxy each time)

# Stream assembled chunks to the browser progressively
streaming: true

# File extensions that always use single-fetch (no chunking)
no_chunk_extensions:
  - ".html"
  - ".json"
  - ".xml"
  - ".js"
  - ".css"
  - ".txt"
  # ... (see config.example.yaml for full list)

# MIME type prefixes that always use single-fetch
no_chunk_mimetypes:
  - "text/"
  - "application/json"
  - "application/javascript"
  # ... (see config.example.yaml for full list)

# ProxyScrape JSON API for --use-free-list
free_proxy_api: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=json"
free_proxy_file: "free-proxy-list.txt"

proxies:
  - "http://1.2.3.4:8080"
  - "http://user:pass@5.6.7.8:3128"
  - "socks5://9.10.11.12:1080"
  - "socks5://user:pass@13.14.15.16:1080"
```

At least **2 proxies** must be live at startup — enforced on launch.

---

## CLI

```bash
# Basic
python proxy_server.py

# Custom config
python proxy_server.py --config custom.yaml

# Load proxies from a text file (one URL per line; repeatable)
python proxy_server.py --proxy-file proxies.txt
python proxy_server.py --proxy-file http.txt --proxy-file socks5.txt

# Download fresh free proxies from the API (prompts if list already exists)
python proxy_server.py --use-free-list

# Combine everything
python proxy_server.py --config custom.yaml --proxy-file extra.txt --use-free-list

# Verbosity
python proxy_server.py --log-level DEBUG    # per-chunk detail
python proxy_server.py --log-level WARNING  # errors only
```

| Flag                | Short | Default       | Description                                          |
|---------------------|-------|---------------|------------------------------------------------------|
| `--config FILE`     | `-c`  | `config.yaml` | YAML config file                                     |
| `--proxy-file FILE` | `-f`  | —             | Text file of proxy URLs, one per line. Repeatable.   |
| `--use-free-list`   |       | off           | Download proxies from ProxyScrape API (prompts if file exists) |
| `--log-level`       |       | `INFO`        | `DEBUG` / `INFO` / `WARNING` / `ERROR`               |

---

## Free Proxy List

```bash
python proxy_server.py --use-free-list
```

- If `free-proxy-list.txt` does **not** exist → downloads from the ProxyScrape v4 JSON API and saves it.
- If `free-proxy-list.txt` **already exists** → asks:
  ```
  📋 Free proxy list already exists: free-proxy-list.txt
     Re-download from API? [y/n]:
  ```
  Answer `y` to refresh or `n` to reuse the cached file.

The API URL is configurable via `free_proxy_api` in `config.yaml`.

---

## Proxy File Format

One URL per line. Blank lines and `#` comments are ignored:

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

## Monitoring

| Endpoint  | Method | Description                                                    |
|-----------|--------|----------------------------------------------------------------|
| `/health` | GET    | `200 ok` when ≥2 live proxies, `503` otherwise                 |
| `/status` | GET    | JSON: uptime, proxy count + list, chunk size, worker count     |

```bash
curl http://localhost:8888/health
curl http://localhost:8888/status | python -m json.tool
```

---

## How It Works

1. **HEAD** — discovers `Content-Length`, `Accept-Ranges`, and `Content-Type`; retried across multiple proxies
2. **Exclusion check** — if the URL extension or `Content-Type` is in the exclusion list, skips to single-fetch
3. **Fallback** — if no `Content-Length` or `Accept-Ranges: bytes`, falls back to single-fetch
4. **Split** — file is divided into N chunks of `chunk_size` bytes using byte ranges
5. **Distribute** — proxies assigned so no two consecutive chunks share the same proxy
6. **Parallel fetch** — all chunks fetched concurrently, bounded by `max_workers`
7. **Retry** — failed chunks retried with a fresh proxy up to `chunk_retries` times
8. **Stream** — chunks written to the browser socket in order as they land in memory

---

## Changelog

### v3 (current)
- **True browser streaming** — `StreamResponse` streams chunks to the client progressively instead of buffering the full file
- **Extension + MIME exclusion list** — HTML, JSON, XML, JS, CSS, SVG and other text types automatically bypass chunking
- **ProxyScrape JSON API integration** — `--use-free-list` downloads from the live API; re-download prompt when file already exists
- **`streaming`, `no_chunk_extensions`, `no_chunk_mimetypes`, `free_proxy_api`, `free_proxy_file`** config keys added
- **`.gitignore`** added — excludes `config.yaml`, `free-proxy-list.txt`, venvs, caches, IDE files

### v2
- Session pool — `ClientSession` reused per proxy URL across chunks
- HEAD retries across multiple proxies (no single-proxy SPOF)
- Async startup health + anonymity check (transparent proxies filtered out)
- Assembled length verification — 502 on mismatch
- POST / PUT / PATCH body forwarding
- `/health` and `/status` endpoints
- `--log-level` flag; per-chunk logs moved to DEBUG
- `config.example.yaml` added
