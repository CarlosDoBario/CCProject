#!/usr/bin/env python3
"""
Diagnóstico: envia uma amostra de telemetria e consulta o endpoint /api/rovers.

Uso (PowerShell):
  $env:PYTHONPATH="src"; python src\scripts\diagnose_send_and_query.py

Uso (Bash):
  PYTHONPATH=src python src/scripts/diagnose_send_and_query.py
"""
import asyncio
import time
from urllib import request
from common import binary_proto, config
from rover.telemetry_client import send_once

async def main():
    host, port = config.TELEMETRY_HOST, config.TELEMETRY_PORT
    print("Sending telemetry to", host, port)
    rover_id = "R-DIAG-01"
    tel = {
        "timestamp_ms": binary_proto.now_ms(),
        "position": {"x": 1.0, "y": 2.0, "z": 0.0},
        "battery_level_pct": 77,
        "status": "IN_MISSION",
        "progress_pct": 12.5,
    }
    try:
        await send_once(host, port, rover_id, tel, ack_requested=False, include_crc=False)
        print("Sent telemetry OK")
    except Exception as e:
        print("Error sending telemetry:", e)
        return

    # wait briefly for server & broadcast
    time.sleep(0.5)

    # Use 127.0.0.1 when API is bound to 0.0.0.0
    api_host = config.API_HOST if config.API_HOST != "0.0.0.0" else "127.0.0.1"
    url = f"http://{api_host}:{config.API_PORT}/api/rovers"
    try:
        print("Querying API:", url)
        resp = request.urlopen(url, timeout=2).read().decode()
        print("API /api/rovers response:", resp)
    except Exception as e:
        print("Failed to query API:", e)

if __name__ == "__main__":
    asyncio.run(main())