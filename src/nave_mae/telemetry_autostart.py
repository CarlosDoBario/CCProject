"""
Helper to start TelemetryServer in background from the ML server.

This module schedules telemetry startup as a background task and is compatible with
the updated telemetry_launcher.start_telemetry_server API (which supports standalone mode).

Usage (integrated):
    # After creating MissionStore instance `ms` and having an event loop:
    loop.create_task(start_telemetry_background(ms, host="127.0.0.1", port=65080))

Place at: src/nave_mae/telemetry_autostart.py
"""
import asyncio
import logging
from typing import Optional

from nave_mae.telemetry_store import TelemetryStore
from nave_mae.telemetry_launcher import start_telemetry_server

_logger = logging.getLogger("ml.telemetry_autostart")


async def start_telemetry_background(mission_store, host: str = "127.0.0.1", port: int = 65080) -> None:
    """
    Create TelemetryStore, register hooks and start TelemetryServer bound to host:port.
    Intended to be scheduled as a background task in the same event loop as the ML server.
    """
    try:
        ts = TelemetryStore()
        # start_telemetry_server supports passing an existing mission_store and telemetry_store;
        # returning the running server object in the "telemetry_server" field
        services = await start_telemetry_server(mission_store=mission_store, telemetry_store=ts, host=host, port=port)
        _logger.info("TelemetryServer started on %s:%d (background task)", host, port)
        # Keep running until cancelled; start_telemetry_server already awaited server.start()
        # We return only when the server stops (start_telemetry_server returns only after start())
    except asyncio.CancelledError:
        _logger.info("TelemetryServer background startup cancelled")
        raise
    except Exception:
        _logger.exception("Failed to start TelemetryServer in background")