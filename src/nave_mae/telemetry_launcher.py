"""
telemetry_launcher.py

Small helper script / library to start a TelemetryServer (TelemetryStream) for the
Nave-Mãe together with TelemetryStore and MissionStore.

Provides:
- start_telemetry_server(mission_store, telemetry_store, host, port)
- run_server_forever(host, port) - CLI entrypoint that starts services and runs until Ctrl-C

Place at: src/nave_mae/telemetry_launcher.py
"""
import asyncio
import logging
from typing import Optional, Tuple

from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore
from nave_mae.telemetry_server import TelemetryServer
from nave_mae.telemetry_hooks import register_telemetry_hooks

_logger = logging.getLogger("ml.telemetry_launcher")


async def start_telemetry_server(
    mission_store: MissionStore,
    telemetry_store: TelemetryStore,
    host: str = "127.0.0.1",
    port: int = 65080,
) -> TelemetryServer:
    """
    Create and start a TelemetryServer, register telemetry hooks and return the server instance.
    The caller is responsible for later calling server.stop().
    """
    # register integration hooks (update MissionStore on telemetry updates)
    try:
        register_telemetry_hooks(mission_store, telemetry_store)
    except Exception:
        _logger.exception("Failed to register telemetry hooks")

    server = TelemetryServer(mission_store=mission_store, telemetry_store=telemetry_store, host=host, port=port)
    await server.start()
    return server


def run_server_forever(host: str = "127.0.0.1", port: int = 65080) -> None:
    """
    Simple CLI helper: create default MissionStore and TelemetryStore, start server and block.
    Use Ctrl-C to stop.
    """
    loop = asyncio.get_event_loop()
    ms = MissionStore()
    ts = TelemetryStore()
    try:
        server = loop.run_until_complete(start_telemetry_server(ms, ts, host=host, port=port))
        _logger.info("Telemetry server started; press Ctrl-C to stop")
        loop.run_forever()
    except KeyboardInterrupt:
        _logger.info("Shutting down telemetry server (KeyboardInterrupt)")
    finally:
        try:
            loop.run_until_complete(server.stop())
        except Exception:
            pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start TelemetryServer (Nave-Mãe)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind telemetry server")
    parser.add_argument("--port", type=int, default=65080, help="Port to bind telemetry server")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run_server_forever(host=args.host, port=args.port)