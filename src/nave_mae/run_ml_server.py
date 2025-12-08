#!/usr/bin/env python3
"""
Small CLI to run the ML (missionLink) UDP server (MLServerProtocol) simply,
without using start_all.py.

Usage:
  PYTHONPATH=src python3 src/nave_mae/run_ml_server.py --host 0.0.0.0 --port 64070
"""
import argparse
import asyncio
import signal
import sys

from nave_mae.ml_server import MLServerProtocol
from nave_mae.mission_store import MissionStore

STOP = False
def _on_sig(sig, frame):
    global STOP
    STOP = True

signal.signal(signal.SIGINT, _on_sig)
signal.signal(signal.SIGTERM, _on_sig)

async def main(host: str, port: int):
    loop = asyncio.get_running_loop()
    ms = MissionStore()
    protocol = MLServerProtocol(ms)
    transport, _ = await loop.create_datagram_endpoint(lambda: protocol, local_addr=(host, port))
    print(f"MLServer listening on {host}:{port} (press Ctrl-C to stop)")
    try:
        while not STOP:
            await asyncio.sleep(0.5)
    finally:
        try:
            # request graceful stop of protocol background tasks
            await protocol.stop(wait_timeout=2.0)
        except Exception:
            pass
        try:
            transport.close()
        except Exception:
            pass
        print("MLServer stopped")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=64070)
    args = p.parse_args()
    try:
        asyncio.run(main(args.host, args.port))
    except KeyboardInterrupt:
        sys.exit(0)