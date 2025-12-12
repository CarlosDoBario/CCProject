"""
Centralized configuration for the project.

This module provides a single source of truth for runtime-configurable parameters
used by the ML server, TelemetryServer and related components.

Usage:
    from common import config
    host = config.TELEMETRY_HOST
    mission_file = config.MISSION_STORE_FILE
    config.configure_logging()   # optional: ensure logging configured with config.LOG_LEVEL

Behavior:
- Values are read from environment variables (see names below) with sensible defaults.
- DATA_DIR is created on import (os.makedirs(..., exist_ok=True)).
- Small parsing helpers are provided for robust environment parsing.
"""

from __future__ import annotations

import logging
import os
import typing
from typing import Any, Optional

# Helper parsers --------------------------------------------------------------
def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in ("1", "true", "t", "yes", "y", "on"):
        return True
    if v in ("0", "false", "f", "no", "n", "off"):
        return False
    return default

def _parse_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(float(value))  # allow "1.0" -> 1
    except Exception:
        return default

def _parse_float(value: Optional[str], default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default

def _parse_str(value: Optional[str], default: str) -> str:
    if value is None:
        return default
    return str(value)

# Environment-aware configuration --------------------------------------------
# DATA_DIR: default ./data or ML_DATA_DIR env var
_DATA_DIR_RAW = os.environ.get("ML_DATA_DIR") or os.environ.get("DATA_DIR") or "./data"
DATA_DIR: str = os.path.abspath(os.path.expanduser(_parse_str(_DATA_DIR_RAW, "./data")))

# Ensure data directory exists on import
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    # best-effort: if can't create, leave it and let consumers handle IO errors
    pass

# MISSION_STORE_FILE: env ML_MISSION_STORE_FILE else DATA_DIR/mission_store.json
_MSF_ENV = os.environ.get("ML_MISSION_STORE_FILE")
MISSION_STORE_FILE: str = os.path.abspath(
    os.path.expanduser(_parse_str(_MSF_ENV, os.path.join(DATA_DIR, "mission_store.json")))
)

# Protocol identifiers and binary protocol version
TS_PROTOCOL: str = _parse_str(os.environ.get("TS_PROTOCOL"), "TS/1.0")
ML_PROTOCOL: str = _parse_str(os.environ.get("ML_PROTOCOL"), "ML/1.0")
BINARY_PROTO_VERSION: int = _parse_int(os.environ.get("BINARY_PROTO_VERSION"), 1)

# Networking defaults
TELEMETRY_HOST: str = _parse_str(os.environ.get("TELEMETRY_HOST"), "127.0.0.1")
TELEMETRY_PORT: int = _parse_int(os.environ.get("TELEMETRY_PORT"), 65080)

ML_HOST: str = _parse_str(os.environ.get("ML_HOST"), "0.0.0.0")
ML_UDP_PORT: int = _parse_int(os.environ.get("ML_UDP_PORT"), 64070)

# API de Observação / Ground Control
API_HOST: str = _parse_str(os.environ.get("API_HOST"), "127.0.0.1")
API_PORT: int = _parse_int(os.environ.get("API_PORT"), 65000)

# Datagram sizing (ML)
ML_MAX_DATAGRAM_SIZE: int = _parse_int(os.environ.get("ML_MAX_DATAGRAM_SIZE"), 1200)

# Time and retry tuning
# Default update interval: choose a reasonable default (30s). Change via env DEFAULT_UPDATE_INTERVAL_S.
DEFAULT_UPDATE_INTERVAL_S: float = _parse_float(os.environ.get("DEFAULT_UPDATE_INTERVAL_S"), 30.0)
TIMEOUT_TX_INITIAL: float = _parse_float(os.environ.get("TIMEOUT_TX_INITIAL"), 1.0)
N_RETX: int = _parse_int(os.environ.get("N_RETX"), 3)
BACKOFF_FACTOR: float = _parse_float(os.environ.get("BACKOFF_FACTOR"), 2.0)

DEDUPE_RETENTION_S: int = _parse_int(os.environ.get("DEDUPE_RETENTION_S"), 60)
DEDUPE_CLEANUP_INTERVAL_S: int = _parse_int(os.environ.get("DEDUPE_CLEANUP_INTERVAL_S"), 10)
PENDING_CLEANUP_INTERVAL_S: int = _parse_int(os.environ.get("PENDING_CLEANUP_INTERVAL_S"), 60)

# Heartbeat / RTT estimation
HEARTBEAT_INTERVAL_S: int = _parse_int(os.environ.get("HEARTBEAT_INTERVAL_S"), 30)
RTT_EWMA_ALPHA: float = _parse_float(os.environ.get("RTT_EWMA_ALPHA"), 0.125)

# Logging / metrics
# Prefer component-specific env vars and then fall back to generic LOG_LEVEL
_LOG_LEVEL_RAW = os.environ.get("TELEMETRY_LOG_LEVEL") or os.environ.get("ML_LOG_LEVEL") or os.environ.get("LOG_LEVEL") or "INFO"
LOG_LEVEL: str = _parse_str(_LOG_LEVEL_RAW, "INFO").upper()
METRICS_ENABLE: bool = _parse_bool(os.environ.get("METRICS_ENABLE"), True)

# Small utility to configure logging if not already configured
def configure_logging(level: Optional[str] = None, fmt: Optional[str] = None) -> None:
    """
    Configure root logging using LOG_LEVEL (or provided level) if logging is not already configured.

    This will call logging.basicConfig(...) only when the root logger has no handlers.
    Call early in your application's entrypoint to ensure consistent log formatting/levels.

    Args:
        level: optional override for log level (string or numeric). If not provided, uses config.LOG_LEVEL.
        fmt: optional format string for logs.
    """
    chosen = level or LOG_LEVEL
    # Accept numeric levels too
    if isinstance(chosen, (int, float)):
        numeric_level = int(chosen)
    else:
        numeric_level = getattr(logging, str(chosen).upper(), logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=numeric_level, format=fmt or "%(asctime)s %(levelname)s [%(name)s] %(message)s")
    else:
        # If handlers exist, set level on root logger to ensure minimum level
        root.setLevel(numeric_level)

# Expose a small API for consumers
__all__ = [
    "DATA_DIR",
    "MISSION_STORE_FILE",
    "TS_PROTOCOL",
    "ML_PROTOCOL",
    "BINARY_PROTO_VERSION",
    "TELEMETRY_HOST",
    "TELEMETRY_PORT",
    "ML_HOST",
    "ML_UDP_PORT",
    "API_HOST",
    "API_PORT",
    "ML_MAX_DATAGRAM_SIZE",
    "DEFAULT_UPDATE_INTERVAL_S",
    "TIMEOUT_TX_INITIAL",
    "N_RETX",
    "BACKOFF_FACTOR",
    "DEDUPE_RETENTION_S",
    "DEDUPE_CLEANUP_INTERVAL_S",
    "PENDING_CLEANUP_INTERVAL_S",
    "HEARTBEAT_INTERVAL_S",
    "RTT_EWMA_ALPHA",
    "LOG_LEVEL",
    "METRICS_ENABLE",
    "configure_logging",
    # helper parsers exported for tests/tools
    "_parse_bool",
    "_parse_int",
    "_parse_float",
    "_parse_str",
]

# ---- Backwards compatibility with previous config.py names ----
# These aliases make old imports/variables keep working while we centralize config.
# Minimal and consistent: expose old names pointing to the above values.

# Protocol / legacy names
# ML_PROTOCOL already defined above from env if present; keep as alias
# ML_UDP_PORT already defined above as ML_UDP_PORT
# ML_MAX_DATAGRAM_SIZE already defined above

# Retransmission / reliability aliases
ACK_JITTER = _parse_float(os.environ.get("ACK_JITTER"), 0.5)

# Misc compatibility aliases
ENV = _parse_str(os.environ.get("ENV"), "development")

