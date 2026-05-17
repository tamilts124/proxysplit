"""
proxy/disk_cache.py
Disk-backed response cache for large files (> in-memory threshold).

Uses sqlite3 with WAL mode as a slab store so gigabyte-sized responses
can be cached across server restarts without RAM cost.  Bodies are stored
as BLOBs; the WAL journal makes concurrent reads safe alongside the
periodic write loop.

When to use
-----------
Set disk_cache_path in config.yaml to enable.  The in-memory LRU cache
(cache.py) still handles small, hot responses.  This cache is consulted
only when the in-memory cache misses and the URL was previously stored on
disk.

Configuration (config.yaml)
---------------------------
disk_cache_path:        "/var/cache/proxy/disk_cache.db"  # required to enable
disk_cache_max_size_mb: 4096          # max DB size in MB, default 4 GB
disk_cache_default_ttl: 86400         # seconds, default 24 h
disk_cache_max_body_mb: 2048          # per-entry body cap in MB, default 2 GB

Thread-safety: all public methods hold a threading.Lock; safe to call
from asyncio coroutines via run_in_executor.
"""

import sqlite3
import threading
import time
from typing import Optional

from proxy.logging_setup import log

_DDL = """
CREATE TABLE IF NOT EXISTS responses (
    url         TEXT PRIMARY KEY,
    status      INTEGER NOT NULL,
    body        BLOB NOT NULL,
    headers     TEXT NOT NULL,
    expires     REAL NOT NULL,
    cached_at   REAL NOT NULL,
    size        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_expires ON responses(expires);
"""

_PRAGMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-32768;
PRAGMA temp_store=MEMORY;
"""


class DiskCache:
    """
    Sqlite3-backed HTTP response cache for large bodies.

    Public API mirrors cache.ResponseCache (get / put / invalidate / clear).
    """

    def __init__(
        self,
        db_path: str,
        max_size_bytes: int = 4 * 1024 ** 3,
        default_ttl: float  = 86400.0,
        max_body: int       = 2 * 1024 ** 3,
    ):
        self.db_path      = db_path
        self.max_size     = max_size_bytes
        self.default_ttl  = default_ttl
        self.max_body     = max_body
        self._lock        = threading.Lock()
        self._hits        = 0
        self._misses      = 0
        self._conn: Optional[sqlite3.Connection] = None
        self._open()
        log.info(
            f"DiskCache: {db_path}  "
            f"max={max_size_bytes // (1024**3)}GB  "
            f"ttl={default_ttl:.0f}s  "
            f"max_body={max_body // (1024**3)}GB"
        )

    def _open(self):
        import json as _json
        self._json = _json
        self._conn = sqlite3.connect(
            self.db_path, check_same_thread=False, timeout=30)
        self._conn.executescript(_PRAGMA)
        self._conn.executescript(_DDL)
        self._conn.commit()

    # ── public API ────────────────────────────────────────────────────────────

    def get(self, url: str) -> Optional[tuple[int, bytes, dict]]:
        """Return (status, body, headers) or None on miss/expiry."""
        import json as _json
        now = time.monotonic()
        with self._lock:
            try:
                cur = self._conn.execute(
                    "SELECT status, body, headers, expires FROM responses WHERE url=?",
                    (url,))
                row = cur.fetchone()
            except sqlite3.Error as exc:
                log.warning(f"DiskCache get error: {exc}")
                self._misses += 1
                return None

            if row is None:
                self._misses += 1
                return None

            status, body, headers_json, expires = row
            if now > expires:
                self._conn.execute("DELETE FROM responses WHERE url=?", (url,))
                self._conn.commit()
                self._misses += 1
                return None

            self._hits += 1
            log.debug(f"DiskCache HIT {url}")
            return status, bytes(body), _json.loads(headers_json)

    def put(self, url: str, status: int, body: bytes, headers: dict):
        """Store a response; skips if body is too large or Cache-Control says no."""
        import json as _json

        if len(body) > self.max_body:
            log.debug(
                f"DiskCache SKIP (body {len(body)//(1024**2)}MB > "
                f"max {self.max_body//(1024**2)}MB) {url}"
            )
            return

        cc = headers.get("Cache-Control", "").lower()
        if "no-store" in cc or "no-cache" in cc or "private" in cc:
            return

        ttl = self.default_ttl
        for part in cc.split(","):
            part = part.strip()
            if part.startswith("max-age="):
                try: ttl = float(part[8:])
                except ValueError: pass
                break
        if ttl <= 0:
            return

        now = time.monotonic()
        with self._lock:
            try:
                self._conn.execute(
                    """INSERT OR REPLACE INTO responses
                       (url, status, body, headers, expires, cached_at, size)
                       VALUES (?,?,?,?,?,?,?)""",
                    (url, status, body, _json.dumps(headers),
                     now + ttl, time.time(), len(body)),
                )
                self._conn.commit()
                self._enforce_limit()
                log.debug(f"DiskCache STORE ttl={ttl:.0f}s size={len(body)//1024}KB {url}")
            except sqlite3.Error as exc:
                log.warning(f"DiskCache put error: {exc}")

    def _enforce_limit(self):
        """Evict oldest entries until DB is within max_size. Caller holds lock."""
        try:
            cur = self._conn.execute("SELECT SUM(size) FROM responses")
            row = cur.fetchone()
            total = row[0] or 0
            while total > self.max_size:
                old = self._conn.execute(
                    "SELECT url, size FROM responses ORDER BY cached_at ASC LIMIT 1"
                ).fetchone()
                if not old:
                    break
                evict_url, evict_sz = old
                self._conn.execute("DELETE FROM responses WHERE url=?", (evict_url,))
                total -= evict_sz
                log.debug(f"DiskCache EVICT (LRU) {evict_url}")
            self._conn.commit()
        except sqlite3.Error as exc:
            log.warning(f"DiskCache enforce_limit error: {exc}")

    def invalidate(self, url: str):
        with self._lock:
            try:
                self._conn.execute("DELETE FROM responses WHERE url=?", (url,))
                self._conn.commit()
            except sqlite3.Error:
                pass

    def clear(self):
        with self._lock:
            try:
                self._conn.execute("DELETE FROM responses")
                self._conn.commit()
                self._hits = self._misses = 0
            except sqlite3.Error as exc:
                log.warning(f"DiskCache clear error: {exc}")

    def vacuum(self):
        """Reclaim disk space after bulk deletes. Runs outside WAL transaction."""
        with self._lock:
            try:
                self._conn.execute("VACUUM")
            except sqlite3.Error as exc:
                log.warning(f"DiskCache vacuum error: {exc}")

    @property
    def stats(self) -> dict:
        with self._lock:
            try:
                cur  = self._conn.execute(
                    "SELECT COUNT(*), SUM(size) FROM responses WHERE expires > ?",
                    (time.monotonic(),))
                count, total = cur.fetchone()
                return {
                    "entries": count or 0,
                    "size_mb": round((total or 0) / (1024 ** 2), 1),
                    "max_mb":  round(self.max_size / (1024 ** 2), 1),
                    "hits":    self._hits,
                    "misses":  self._misses,
                    "hit_rate": round(
                        self._hits / max(1, self._hits + self._misses), 3),
                    "db_path": self.db_path,
                }
            except sqlite3.Error:
                return {"error": "unavailable"}

    def close(self):
        with self._lock:
            if self._conn:
                try: self._conn.close()
                except Exception: pass
                self._conn = None


# ── Module-level singleton — set by init() ───────────────────────────────────
DISK_CACHE: Optional[DiskCache] = None


def init(
    db_path: str,
    max_size_mb: int  = 4096,
    default_ttl: float = 86400.0,
    max_body_mb: int  = 2048,
):
    """Called from proxy_server.main() when disk_cache_path is configured."""
    global DISK_CACHE
    DISK_CACHE = DiskCache(
        db_path        = db_path,
        max_size_bytes = max_size_mb * 1024 * 1024,
        default_ttl    = default_ttl,
        max_body       = max_body_mb * 1024 * 1024,
    )
    return DISK_CACHE
