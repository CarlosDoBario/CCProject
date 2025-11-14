#!/usr/bin/env python3
"""
telemetry_client.py

Simple asyncio TCP client that implements a TelemetryStream (TS) rover-side sender.
Sends JSON-lines (one JSON object per newline) to the TelemetryServer.

Features:
- Optional initial "hello" message to register the rover with the server.
- Periodic telemetry messages containing at least: rover_id, ts, position, state.
- Uses RoverSim (if available) to generate realistic telemetry for demos/tests.
- Reconnect/backoff on connection failure and best-effort ack read.
- CLI friendly for use in tests and manual demos.

Place at: src/rover/telemetry_client.py
"""

import asyncio
import json
import time
import socket
import argparse
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from common import config, utils

# Try to import RoverSim if available (used to generate telemetry)
try:
    from rover.rover_sim import RoverSim
except Exception:
    RoverSim = None  # allow client to run without simulation

logger = utils.get_logger("telemetry.client")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelemetryClient:
    def __init__(
        self,
        rover_id: str,
        host: str = "127.0.0.1",
        port: int = 65080,
        interval_s: float = 1.0,
        hello_first: bool = True,
        use_sim: bool = True,
    ):
        self.rover_id = rover_id
        self.host = host
        self.port = port
        self.interval_s = interval_s
        self.hello_first = hello_first
        self.use_sim = use_sim and RoverSim is not None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._stop = False
        self._sim = None
        if self.use_sim:
            # initialize RoverSim at origin by default
            try:
                self._sim = RoverSim(rover_id, position=(0.0, 0.0, 0.0))
            except Exception:
                self._sim = None
                logger.exception("Failed to initialize RoverSim; telemetry will be synthetic")

    async def connect(self, timeout: float = 5.0) -> bool:
        """Attempt to connect to telemetry server. Returns True on success."""
        try:
            logger.info("Connecting to telemetry server %s:%d", self.host, self.port)
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=timeout
            )
            logger.info("Connected to telemetry server %s:%d", self.host, self.port)
            return True
        except Exception:
            logger.exception("Failed to connect to telemetry server %s:%d", self.host, self.port)
            self._reader = None
            self._writer = None
            return False

    async def close(self) -> None:
        self._stop = True
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._reader = None
            self._writer = None
        logger.info("Telemetry client closed")

    async def send_json_line(self, obj: Dict[str, Any]) -> None:
        if not self._writer:
            raise RuntimeError("Not connected")
        try:
            s = json.dumps(obj, ensure_ascii=False)
            self._writer.write((s + "\n").encode("utf-8"))
            await self._writer.drain()
            logger.debug("Sent telemetry: %s", s)
        except Exception:
            logger.exception("Failed to send telemetry")

    async def read_ack_once(self, timeout: float = 0.1) -> Optional[Dict[str, Any]]:
        """Best-effort single line read (ack) from server with short timeout."""
        if not self._reader:
            return None
        try:
            line = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
            if not line:
                return None
            try:
                return json.loads(line.decode("utf-8").strip())
            except Exception:
                logger.debug("Received non-json ack from server: %r", line)
                return None
        except asyncio.TimeoutError:
            return None
        except Exception:
            logger.exception("Error while reading ack")
            return None

    def _build_telemetry(self) -> Dict[str, Any]:
        if self._sim:
            # simulate one step (small deterministic step)
            try:
                self._sim.step(self.interval_s)
                tel = self._sim.get_telemetry()
                payload = {
                    "type": "telemetry",
                    "rover_id": self.rover_id,
                    "ts": now_iso(),
                    "position": tel.get("position", {}),
                    "state": tel.get("status") or "UNKNOWN",
                    "battery_pct": tel.get("battery_level_pct"),
                    "speed_m_s": tel.get("speed_m_s") if "speed_m_s" in tel else None,
                    "samples_collected": tel.get("samples_collected", 0),
                }
                return payload
            except Exception:
                logger.exception("RoverSim telemetry generation failed; falling back to basic payload")
        # fallback synthetic telemetry
        return {
            "type": "telemetry",
            "rover_id": self.rover_id,
            "ts": now_iso(),
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "state": "IDLE",
            "battery_pct": 100.0,
        }

    async def run_forever(self, max_reconnect_attempts: int = 0) -> None:
        """Main loop: connect, optionally send hello, then periodic telemetry until stopped."""
        reconnect_attempts = 0
        backoff = 0.5
        while not self._stop:
            ok = await self.connect()
            if not ok:
                reconnect_attempts += 1
                if 0 < max_reconnect_attempts <= reconnect_attempts:
                    logger.error("Max reconnect attempts reached, stopping")
                    return
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 1.7)
                continue

            # reset reconnect attempts after successful connect
            reconnect_attempts = 0
            backoff = 0.5

            # optional hello
            if self.hello_first:
                hello = {"type": "hello", "rover_id": self.rover_id, "ts": now_iso(), "meta": {"client": "telemetry_client"}}
                try:
                    await self.send_json_line(hello)
                    # read optional hello_ack
                    await self.read_ack_once(timeout=0.2)
                except Exception:
                    logger.debug("hello exchange failed; continuing")

            try:
                while not self._stop:
                    tel = self._build_telemetry()
                    # send telemetry and optionally wait for ack (best-effort)
                    try:
                        await self.send_json_line(tel)
                    except Exception:
                        logger.exception("Send failed, will attempt reconnect")
                        break
                    # read best-effort ack (not required)
                    await self.read_ack_once(timeout=0.05)
                    # sleep for interval
                    await asyncio.sleep(self.interval_s)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in telemetry loop; reconnecting")
            finally:
                await self.close()

    async def run_once(self) -> None:
        """Connect, send a single telemetry message, and exit."""
        ok = await self.connect()
        if not ok:
            raise RuntimeError("Failed to connect")
        try:
            if self.hello_first:
                await self.send_json_line({"type": "hello", "rover_id": self.rover_id, "ts": now_iso()})
                await self.read_ack_once(timeout=0.2)
            tel = self._build_telemetry()
            await self.send_json_line(tel)
            # allow ack to arrive
            await self.read_ack_once(timeout=0.2)
        finally:
            await self.close()


async def _main_async(args):
    client = TelemetryClient(
        rover_id=args.rover_id,
        host=args.host,
        port=args.port,
        interval_s=args.interval,
        hello_first=not args.no_hello,
        use_sim=not args.no_sim,
    )
    if args.once:
        await client.run_once()
    else:
        await client.run_forever(max_reconnect_attempts=args.max_reconnect)


def main():
    parser = argparse.ArgumentParser(description="TelemetryStream (TS) client simulator")
    parser.add_argument("--rover-id", required=True, help="Rover identifier (e.g. R-001)")
    parser.add_argument("--host", default="127.0.0.1", help="Telemetry server host")
    parser.add_argument("--port", type=int, default=getattr(config, "TELEMETRY_PORT", 65080), help="Telemetry server port")
    parser.add_argument("--interval", type=float, default=1.0, help="Telemetry send interval in seconds")
    parser.add_argument("--once", action="store_true", help="Send a single telemetry message and exit")
    parser.add_argument("--no-hello", action="store_true", help="Do not send an initial hello message")
    parser.add_argument("--no-sim", action="store_true", help="Do not use RoverSim even if available")
    parser.add_argument("--max-reconnect", type=int, default=0, help="Maximum reconnect attempts (0 = infinite)")
    args = parser.parse_args()

    try:
        asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        logger.info("Telemetry client interrupted by user")
    except Exception:
        logger.exception("Telemetry client failed")


if __name__ == "__main__":
    main()