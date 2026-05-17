"""
proxy/config_loader.py
YAML config loading, proxy-file loading, proxy collection,
free-list download, and CLI argument parsing.
"""

import argparse
import json as _json_mod
import logging
import os
import sys
from typing import Optional

import yaml

from proxy.logging_setup import log
from proxy.registry import PROXY_TAGS


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_proxy_file(path: str) -> list[str]:
    proxies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    log.info(f"   Loaded {len(proxies)} proxies from {path}")
    return proxies


def collect_proxies(args, config: dict) -> list[str]:
    seen: set[str] = set()
    proxies: list[str] = []

    def _add(p: str):
        if p not in seen:
            proxies.append(p); seen.add(p)

    for entry in config.get("proxies", []):
        if isinstance(entry, dict):
            url = entry.get("url", "")
            if url:
                _add(url)
                PROXY_TAGS[url] = set(entry.get("tags", []))
        elif isinstance(entry, str):
            _add(entry)

    for pf in args.proxy_files:
        for p in load_proxy_file(pf):
            _add(p)

    if args.use_free_list:
        _load_free_list(args, config, _add)

    return proxies


def _load_free_list(args, config: dict, _add):
    config_dir = os.path.dirname(os.path.abspath(args.config))
    fname = config.get("free_proxy_file", "free-proxy-list.txt")
    fpath = os.path.join(config_dir, fname)
    api_url = config.get(
        "free_proxy_api",
        "https://api.proxyscrape.com/v4/free-proxy-list/get"
        "?request=display_proxies&proxy_format=protocolipport&format=json",
    )
    should_dl = True
    if os.path.exists(fpath):
        if sys.stdin.isatty():
            print(f"\n📋 Free proxy list exists: {fpath}")
            while True:
                c = input("   Re-download? [y/n]: ").strip().lower()
                if c in ("y", "yes"): break
                if c in ("n", "no"): should_dl = False; break
                print("   y or n please.")
        else:
            # Non-interactive (Docker, systemd, CI) — skip re-download by default.
            # Set free_proxy_redownload: true in config.yaml to force it.
            should_dl = config.get("free_proxy_redownload", False)
            log.info(
                f"   Free proxy list exists (headless mode): "
                f"{'re-downloading' if should_dl else 'reusing'} {fpath}"
            )
    if should_dl:
        log.info(f"Downloading free proxies from {api_url}")
        try:
            import urllib.request
            with urllib.request.urlopen(api_url, timeout=30) as resp:
                raw = resp.read().decode()
            data = _json_mod.loads(raw)
            downloaded: list[str] = []
            if isinstance(data, dict) and "proxies" in data:
                for item in data["proxies"]:
                    if isinstance(item, dict):
                        p     = item.get("proxy") or item.get("ip") or ""
                        proto = item.get("protocol", "http").lower()
                        port  = item.get("port", "")
                        if p and ":" not in p:
                            p = f"{proto}://{p}:{port}"
                    else:
                        p = str(item)
                    if p:
                        downloaded.append(p.strip())
            elif isinstance(data, list):
                downloaded = [str(x).strip() for x in data if x]
            else:
                downloaded = [l.strip() for l in raw.splitlines()
                              if l.strip() and not l.startswith("#")]
            with open(fpath, "w") as f:
                f.write("\n".join(downloaded))
            log.info(f"   Saved {len(downloaded)} proxies → {fpath}")
        except Exception as exc:
            log.error(f"Download failed: {exc}")
            if not os.path.exists(fpath):
                return
    if os.path.exists(fpath):
        for p in load_proxy_file(fpath):
            _add(p)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Chunk-Level Parallel Proxy Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python proxy_server.py
  python proxy_server.py --proxy-file proxies.txt
  python proxy_server.py --check-interval 300
  python proxy_server.py --log-format json
  python proxy_server.py --tor --tor-only --tor-refresh-interval 300

chunk_proxy modes:
  --chunk-proxy    (default) each chunk uses a different proxy
  --no-chunk-proxy all chunks in a request share one proxy

proxy file format (one per line, blanks and # comments ignored):
  http://1.2.3.4:8080
  socks5://user:pass@9.10.11.12:1080
        """,
    )
    parser.add_argument("--config", "-c", default="config.yaml", metavar="FILE")
    parser.add_argument("--proxy-file", "-f", action="append", default=[],
                        metavar="FILE", dest="proxy_files")
    parser.add_argument("--use-free-list", action="store_true", default=False)
    parser.add_argument("--log-level",   default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--log-format",  default=None, choices=["text", "json"],
                        help="Log output format (overrides config)")
    parser.add_argument("--check-interval", type=int, default=None, metavar="SECONDS")

    cg = parser.add_mutually_exclusive_group()
    cg.add_argument("--chunk-proxy",    dest="chunk_proxy", action="store_true",  default=None)
    cg.add_argument("--no-chunk-proxy", dest="chunk_proxy", action="store_false")

    parser.add_argument("--tor",                  action="store_true", default=False)
    parser.add_argument("--tor-instances",        type=int, default=None, metavar="N")
    parser.add_argument("--tor-only",             action="store_true", default=False)
    parser.add_argument("--tor-refresh-interval", type=int, default=None, metavar="SECONDS")
    return parser.parse_args()
