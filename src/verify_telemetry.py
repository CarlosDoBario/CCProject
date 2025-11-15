#!/usr/bin/env python3
"""
verify_telemetry.py

Arranca um TelemetryServer in-process, envia uma mensagem com TelemetryClient,
faz snapshot do TelemetryStore e imprime o estado do MissionStore para validação.

Uso:
  PowerShell:
    $env:PYTHONPATH="src"; python .\verify_telemetry.py
  Bash:
    PYTHONPATH=src python verify_telemetry.py
"""
import asyncio
import logging
from rover.telemetry_client import TelemetryClient
from nave_mae.telemetry_launcher import start_telemetry_server

logging.basicConfig(level=logging.INFO)

async def main():
    # Start telemetry server in-process (standalone mode)
    services = await start_telemetry_server(mission_store=None, telemetry_store=None, host="127.0.0.1", port=65080, persist_file=None)
    ms = services["ms"]
    ts = services["ts"]
    server = services["telemetry_server"]
    print("Telemetry server started in-process")

    # Run a client that sends a single telemetry packet
    client = TelemetryClient(rover_id="R-VERIFY", host="127.0.0.1", port=65080, interval_s=0.01, hello_first=True, use_sim=False)
    await client.run_once()
    # allow small time for server to process hooks
    await asyncio.sleep(0.2)

    # Inspect telemetry store and mission store
    snap = await ts.snapshot()
    print("TelemetryStore snapshot keys:", list(snap.keys()))
    try:
        # MissionStore API may vary; try common methods
        if hasattr(ms, "list_rovers"):
            rovers = ms.list_rovers()
        elif hasattr(ms, "rovers"):
            rovers = list(ms.rovers.keys())
        else:
            rovers = None
        print("MissionStore rovers:", rovers)
    except Exception as e:
        print("Could not inspect mission_store:", e)

    # Stop server
    await server.stop()
    print("Telemetry server stopped")

if __name__ == "__main__":
    asyncio.run(main())