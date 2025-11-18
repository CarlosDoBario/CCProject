#!/usr/bin/env python3
"""
Helper to start TelemetryServer in background from the ML server.

This module schedules telemetry startup as a background task and is compatible with
the updated telemetry_launcher.start_telemetry_server API (which supports standalone mode).

Usage (integrated):
    # After creating MissionStore instance `ms` and having an event loop:
    loop.create_task(start_telemetry_background(ms, host="127.0.0.1", port=65080))

Behaviour:
 - If telemetry_store is provided, it will be used; otherwise a TelemetryStore is created.
 - The background task waits until cancelled; on cancellation it attempts a graceful server.stop().
"""
from __future__ import annotations

import asyncio
from typing import Optional, Any, Tuple

from common import config, utils

logger = utils.get_logger("nave_mae.telemetry_autostart")


async def start_telemetry_background(
    mission_store: Any,
    host: str = None,
    port: Optional[int] = None,
    telemetry_store: Optional[Any] = None,
    persist_file: Optional[str] = None,
) -> None:
    """
    Create TelemetryStore, register hooks and start TelemetryServer bound to host:port.
    Intended to be scheduled as a background task in the same event loop as the ML server.

    This function returns only when the task is cancelled or when the server stops.
    On cancellation it will attempt to stop the TelemetryServer cleanly.
    """
    host = host or config.TELEMETRY_HOST
    port = port if port is not None else config.TELEMETRY_PORT

    try:
        # Lazy import to avoid circular deps
        from nave_mae.telemetry_store import TelemetryStore  # type: ignore
        from nave_mae.telemetry_launcher import start_telemetry_server  # type: ignore
    except Exception:
        logger.exception("Required telemetry modules not available")
        raise

    # Ensure telemetry_store exists
    provided_ts = telemetry_store
    if provided_ts is None:
        try:
            telemetry_store = TelemetryStore()
            logger.debug("Created TelemetryStore for background telemetry server")
        except Exception:
            logger.exception("Failed to create TelemetryStore instance")
            raise

    server = None
    services = None
    try:
        services = await start_telemetry_server(mission_store=mission_store, telemetry_store=telemetry_store, host=host, port=port, persist_file=persist_file)
        server = services.get("telemetry_server")
        logger.info("TelemetryServer started on %s:%d (background task)", host, port)

        # Wait forever (until task is cancelled). Using an Event avoids busy-wait.
        wait_evt = asyncio.Event()
        await wait_evt.wait()

    except asyncio.CancelledError:
        logger.info("TelemetryServer background task cancelled; attempting graceful shutdown")
        # Attempt to stop server cleanly
        try:
            if server and hasattr(server, "stop"):
                maybe_stop = server.stop
                if asyncio.iscoroutinefunction(maybe_stop):
                    await maybe_stop()
                else:
                    # wrap sync stop in executor
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, maybe_stop)
                logger.info("TelemetryServer stopped cleanly")
        except Exception:
            logger.exception("Error while stopping TelemetryServer on cancellation")
        finally:
            # If we created telemetry_store here and it has a close/save method, consider invoking it
            if provided_ts is None:
                # best-effort cleanup if telemetry_store exposes save/close
                try:
                    if hasattr(telemetry_store, "save"):
                        telemetry_store.save()
                except Exception:
                    pass
        raise

    except Exception:
        logger.exception("Failed to start TelemetryServer in background")
        # If startup partially succeeded, try to stop any started server
        try:
            if server and hasattr(server, "stop"):
                if asyncio.iscoroutinefunction(server.stop):
                    await server.stop()
                else:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, server.stop)
        except Exception:
            logger.exception("Error while cleaning up TelemetryServer after failure")
        raise