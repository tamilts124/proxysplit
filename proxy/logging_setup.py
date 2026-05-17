"""
proxy/logging_setup.py
Logging formatter, handler, and global logger.
"""

import json as _json_mod
import logging
import time

_log_handler = logging.StreamHandler()
_log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(handlers=[_log_handler], level=logging.INFO)
log = logging.getLogger("chunk-proxy")

_START_TIME = time.monotonic()


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": round(record.created, 3),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        for key in ("proxy", "chunk", "latency_ms", "url"):
            if hasattr(record, key):
                obj[key] = getattr(record, key)
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        return _json_mod.dumps(obj)


def configure(log_fmt: str, log_level: str):
    """Call once at startup with config values."""
    if log_fmt == "json":
        _log_handler.setFormatter(_JsonFormatter())
    logging.getLogger().setLevel(log_level)
    log.setLevel(log_level)


def suppress_connection_reset(loop, context):
    ex = context.get("exception")
    if isinstance(ex, (ConnectionResetError, OSError)) and getattr(ex, "winerror", None) == 10054:
        return
    loop.default_exception_handler(context)
