#!/usr/bin/env python3
"""
WSGI entry point for Gunicorn/Koyeb.

KEY: /health returns 200 in <100ms. Server loads in background.
All other endpoints wait for server to load (max 30s).

LOGGING:
  - Gunicorn access log: 2xx/3xx suppressed — only 4xx/5xx shown.
  - Oracle/lattice/quantum logs: WARNING+ always visible.
  - Set LOG_200=1 env var to re-enable 200 access logs for debugging.
"""

import logging
import os
import sys
import time
import threading

logger = logging.getLogger(__name__)

# ═══ ACCESS LOG FILTER — suppress 200/3xx, show only errors ═══════════════════

class _ErrorOnlyAccessFilter(logging.Filter):
    """
    Drop Gunicorn access log entries for successful (2xx/3xx) responses.
    Only 400+ status codes pass through — cuts log noise by ~99%.
    Set LOG_200=1 to disable this filter and see all requests.
    """
    _SUPPRESS_PREFIXES = (" 2", " 3")  # status codes in gunicorn format: ' 200 ', ' 301 '

    def filter(self, record: logging.LogRecord) -> bool:
        if os.environ.get("LOG_200") == "1":
            return True  # debug mode: show everything
        msg = record.getMessage()
        # Gunicorn access log format: '10.x.x.x - [...] "GET /path HTTP/1.1" 200 ...'
        # Check for ' 200 ', ' 201 ', ' 204 ', ' 301 ', ' 304 ' etc.
        for prefix in self._SUPPRESS_PREFIXES:
            if f'"{prefix}' in msg or f" {prefix[1]}" in msg:
                # More precise: check the status code field position
                parts = msg.rsplit('"', 1)
                if len(parts) == 2:
                    status_part = parts[1].strip()
                    if status_part and status_part[0] in ('2', '3'):
                        return False
        return True


def _configure_logging():
    """
    Wire up:
      1. Access log filter — 2xx/3xx → /dev/null
      2. Oracle/quantum/lattice logs → WARNING level always visible
      3. App errors → always visible
    """
    # Suppress 200s from gunicorn.access logger
    access_logger = logging.getLogger("gunicorn.access")
    if os.environ.get("LOG_200") != "1":
        _f = _ErrorOnlyAccessFilter()
        access_logger.addFilter(_f)
        # Also suppress via a custom handler that respects the filter
        access_logger.setLevel(logging.WARNING)

    # Oracle/lattice/quantum subsystems — ensure WARNING+ always reaches stdout
    _oracle_namespaces = [
        "oracle", "lattice_controller", "quantum", "ORACLE",
        "ORACLE-MULTIPLEX", "ORACLE CLUSTER", "LATTICE",
    ]
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setLevel(logging.WARNING)
    _fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _handler.setFormatter(_fmt)

    for ns in _oracle_namespaces:
        _ns_logger = logging.getLogger(ns)
        if not _ns_logger.handlers:
            _ns_logger.addHandler(_handler)
        _ns_logger.setLevel(logging.WARNING)

    # Ensure root logger passes WARNING+
    root = logging.getLogger()
    if root.level == logging.NOTSET or root.level > logging.WARNING:
        root.setLevel(logging.WARNING)

    # Flask/werkzeug: suppress their 200 access lines too
    logging.getLogger("werkzeug").setLevel(logging.ERROR)


_configure_logging()

# Add hlwe to path
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_HYP_DIR = os.path.join(_REPO_ROOT, "hlwe")
if _HYP_DIR not in sys.path:
    sys.path.insert(0, _HYP_DIR)

_STARTUP = time.time()

# ═══ INSTANT HEALTH APP - no heavy imports ═══
from flask import Flask

_health_app = Flask("__health__")


@_health_app.route("/health")
def health():
    return "", 200


print(f"[WSGI] ✅ /health ready at {time.time() - _STARTUP:.2f}s", flush=True)

# ═══ LOAD SERVER IN BACKGROUND ═══
_full_app = None
_load_done = threading.Event()


def _load_server():
    global _full_app
    try:
        print("[WSGI] Loading full server module...", flush=True)
        os.environ.setdefault("QTCL_SERVER_MANAGED", "1")
        from server import app as full_app
        _full_app = full_app
        print(
            f"[WSGI] ✅ Full server loaded at {time.time() - _STARTUP:.1f}s", flush=True
        )
    except Exception as e:
        print(f"[WSGI] ❌ Server load failed: {e}", flush=True)
    finally:
        _load_done.set()


_thread = threading.Thread(target=_load_server, daemon=True)
_thread.start()


# ═══ WSGI APP ═══
def application(environ, start_response):
    path = environ.get("PATH_INFO", "/")

    if path in ("/health", "/health/"):
        return _health_app(environ, start_response)

    if not _full_app:
        _load_done.wait(timeout=120)

    if _full_app:
        return _full_app(environ, start_response)

    start_response("503 Service Unavailable", [("Content-Type", "application/json")])
    return [
        b'{"jsonrpc":"2.0","error":{"code":-32000,"message":"Server failed to start"},"id":null}'
    ]


app = application

print(f"[WSGI] ✅ WSGI ready at {time.time() - _STARTUP:.2f}s", flush=True)
print("[WSGI] Access log: 2xx/3xx suppressed (set LOG_200=1 to restore)", flush=True)
