#!/usr/bin/env python3
"""
Demo: envia algumas amostras de telemetria para a Nave-Mãe.
Uso:
  $env:PYTHONPATH="src"; python send_telemetry_demo.py
"""
import asyncio
from common import binary_proto, config
from rover.telemetry_client import send_once  # função async utilitária que o repo já tem

async def main():
    host = config.TELEMETRY_HOST
    port = config.TELEMETRY_PORT
    rover_id = "R-DEMO-01"
    for i in range(3):
        tel = {
            "timestamp_ms": binary_proto.now_ms(),
            "position": {"x": i * 1.0, "y": i * 2.0, "z": 0.0},
            "battery_level_pct": 90 - i * 5,
            "status": "IN_MISSION",
            "progress_pct": float(i) * 10.0,
        }
        # send_once(host, port, rover_id, telemetry, ack_requested=False, include_crc=False)
        await send_once(host, port, rover_id, tel, ack_requested=False, include_crc=False)
        await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())