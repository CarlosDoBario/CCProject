#!/usr/bin/env python3
"""
telemetry_launcher.py

Small helper library to start a TelemetryServer (TelemetryStream) for the
Nave-Mãe. Updated to work with the binary TelemetryServer/TelemetryStore and
the canonical telemetry payloads produced by common.binary_proto.

Behaviour:
 - creates MissionStore and TelemetryStore when not provided
 - registers telemetry -> mission hooks via nave_mae.telemetry_hooks.register_telemetry_hooks
 - starts TelemetryServer and returns handles for tests/cleanup
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Dict, Any

from common import config, utils

_logger = utils.get_logger("ml.telemetry_launcher")


async def start_telemetry_server(
    mission_store: Optional[object] = None,
    telemetry_store: Optional[object] = None,
    host: str = "127.0.0.1",
    port: int = 65080,
    persist_file: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create (or use provided) MissionStore and TelemetryStore, register hooks and start a TelemetryServer.

    Returns a dict containing:
      - "ms": the MissionStore instance used
      - "ts": the TelemetryStore instance used
      - "telemetry_server": the running TelemetryServer instance
    """
    # Lazy imports to avoid circular imports on module import
    try:
        from nave_mae.telemetry_store import TelemetryStore  # type: ignore
        from nave_mae.telemetry_server import TelemetryServer  # type: ignore
        from nave_mae.telemetry_hooks import register_telemetry_hooks  # type: ignore
    except Exception:
        _logger.exception("Telemetry modules could not be imported (are files present?)")
        raise

    # Ensure mission_store exists (standalone mode)
    if mission_store is None:
        try:
            from nave_mae.mission_store import MissionStore  # type: ignore
            # Try to pass persist_file if constructor accepts it
            try:
                mission_store = MissionStore(persist_file=persist_file) if persist_file else MissionStore()
            except TypeError:
                mission_store = MissionStore()
            _logger.info("Created standalone MissionStore for TelemetryServer (persist_file=%s)", persist_file)
        except Exception:
            _logger.exception("Failed to create MissionStore for standalone TelemetryServer")
            raise

    # Ensure telemetry_store exists
    if telemetry_store is None:
        try:
            telemetry_store = TelemetryStore()
            _logger.info("Created TelemetryStore (history_size default)")
        except Exception:
            _logger.exception("Failed to create TelemetryStore")
            raise

    # Try to register hooks before starting server (some telemetry_store impls expose subscription points early)
    try:
        register_telemetry_hooks(mission_store, telemetry_store)
    except Exception:
        _logger.exception("Failed to register telemetry hooks (pre-start) — will try again after server.start()")

    # Create and start TelemetryServer
    server = None
    try:
        server = TelemetryServer(mission_store=mission_store, telemetry_store=telemetry_store, host=host, port=port)
        await server.start()
        _logger.info("TelemetryServer started on %s:%d", host, port)
    except Exception:
        _logger.exception("Failed to start TelemetryServer")
        raise

    # After server.start(), some TelemetryStore or TelemetryServer implementations expose event APIs only then.
    attached = False
    try:
        # First try with the telemetry_store (it may have become usable after start)
        try:
            register_telemetry_hooks(mission_store, telemetry_store)
            attached = True
        except Exception:
            _logger.debug("register_telemetry_hooks failed for telemetry_store (post-start), will try server object", exc_info=False)

        # If not attached, try attaching to the server object itself (some servers expose subscribe/on on the server)
        if not attached:
            try:
                register_telemetry_hooks(mission_store, server)
                attached = True
            except Exception:
                _logger.debug("register_telemetry_hooks failed for telemetry_server object (post-start)", exc_info=False)

        # If server has a nested telemetry_store attribute, try that as well
        if not attached and hasattr(server, "telemetry_store"):
            try:
                register_telemetry_hooks(mission_store, getattr(server, "telemetry_store"))
                attached = True
            except Exception:
                _logger.debug("register_telemetry_hooks failed for server.telemetry_store (post-start)", exc_info=False)

    except Exception:
        _logger.exception("Failed to register telemetry hooks (post-start); telemetry events may not update MissionStore")

    if attached:
        _logger.info("Registered telemetry -> MissionStore hook")
    else:
        _logger.warning("Could not attach telemetry -> MissionStore hook automatically; telemetry events may not update MissionStore")

    return {"ms": mission_store, "ts": telemetry_store, "telemetry_server": server}


def run_server_forever(host: str = "127.0.0.1", port: int = 65080, persist_file: Optional[str] = None) -> None:
    """
    CLI helper to run TelemetryServer standalone (creates its own MissionStore and TelemetryStore).
    Blocks until Ctrl-C.
    """
    # Ensure logging configured consistently with config
    try:
        config.configure_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    services = {}
    try:
        services = loop.run_until_complete(start_telemetry_server(host=host, port=port, persist_file=persist_file))
        _logger.info("Telemetry server started; press Ctrl-C to stop")
        loop.run_forever()
    except KeyboardInterrupt:
        _logger.info("Shutting down telemetry server (KeyboardInterrupt)")
    finally:
        try:
            server = services.get("telemetry_server")
            if server:
                loop.run_until_complete(server.stop())
        except Exception:
            _logger.exception("Error stopping telemetry server")
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start TelemetryServer (Nave-Mãe) standalone")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind telemetry server")
    parser.add_argument("--port", type=int, default=config.TELEMETRY_PORT, help="Telemetry server port")
    parser.add_argument("--persist-file", default=None, help="Optional persist file to pass to MissionStore (standalone)")
    args = parser.parse_args()
    run_server_forever(host=args.host, port=args.port, persist_file=args.persist_file)