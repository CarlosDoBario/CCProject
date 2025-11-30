#!/usr/bin/env python3
"""
Ground Control client (console) - robust WebSocket consumer that applies
MISSION_SNAPSHOT, ROVER_REGISTERED, TELEMETRY, TELEMETRY_UPDATE and similar events
immediately on first receipt.

Execute:
  $env:PYTHONPATH="src"; python -m groundcontrol.gc_cliente

Requisitos:
  pip install websockets
"""
from __future__ import annotations
import asyncio
import json
import os
import logging
import signal
import sys
from typing import Dict, Any, Optional

from common import config

try:
    import websockets
except Exception:
    print("Instala a dependência 'websockets' (pip install websockets) e tenta de novo.")
    raise

# Configure logging from env
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [gc_cliente] %(message)s")
logger = logging.getLogger("gc_cliente")

WS_HOST = "127.0.0.1" if config.API_HOST == "0.0.0.0" else config.API_HOST
WS_URL = f"ws://{WS_HOST}:{config.API_PORT}/ws/subscribe"

# In-memory state
rovers: Dict[str, Dict[str, Any]] = {}     # rover_id -> telemetry/state dict
missions: Dict[str, Any] = {}              # mission_id -> mission data (optional)


def render_screen():
    """Simple console rendering of current missions and rovers state."""
    # Clear screen (basic)
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
    except Exception:
        pass

    print("-" * 60)
    print("🛰️  GROUND CONTROL CLIENT - Status: CONECTADO")
    print("-" * 60)
    # Missions summary
    print("### Missões Ativas / Pendentes:")
    if not missions:
        print("  Nenhuma missão ativa ou pendente.")
    else:
        for mid, m in missions.items():
            state = m.get("state", "UNKNOWN")
            task = m.get("task", "")
            progress = m.get("last_progress_pct", 0.0)
            print(f"  {mid} | {task} | STATE: {state} | PROG: {progress:.1f}%")
    print("-" * 60)
    # Rovers
    print("### Estado e Localização dos Rovers:")
    if not rovers:
        print("  Nenhum Rover online ou registado.")
    else:
        for rid, data in rovers.items():
            pos = data.get("position", {})
            x = pos.get("x", 0.0)
            y = pos.get("y", 0.0)
            z = pos.get("z", 0.0)
            batt = data.get("battery_level_pct", 0.0)
            prog = data.get("progress_pct", 0.0)
            status = data.get("status", "N/A")
            print(f"  {rid} | STATUS: {status:7} | POS: ({x:6.2f}, {y:6.2f}, {z:5.2f}) | BATT: {batt:6.1f}% | PROG: {prog:6.1f}%")
    print("-" * 60)


def apply_telemetry(rover_id: str, telemetry: Dict[str, Any]):
    """Merge telemetry into in-memory rover state and render screen."""
    if rover_id not in rovers:
        rovers[rover_id] = {}
    # Merge fields (flat merge is fine for our console)
    rovers[rover_id].update(telemetry)
    # ensure rover_id present on state
    rovers[rover_id]["rover_id"] = rover_id
    logger.debug("Applied telemetry for %s: %s", rover_id, telemetry)
    render_screen()


def handle_mission_snapshot(data: Dict[str, Any]):
    """Process mission snapshot and optional rovers block."""
    ms = data.get("missions", {}) if isinstance(data, dict) else {}
    rovers_block = data.get("rovers", {}) if isinstance(data, dict) else {}
    # Replace missions state
    missions.clear()
    if isinstance(ms, dict):
        missions.update(ms)
    # Merge rovers if provided
    for rid, latest in (rovers_block or {}).items():
        if isinstance(latest, dict):
            apply_telemetry(rid, latest)
    # Final render
    render_screen()


def handle_rover_registered(data: Dict[str, Any]):
    """Handle rover registration event (ensure rover exists)."""
    rid = data.get("rover_id") or data.get("id") or data.get("id_str")
    if not rid:
        return
    if rid not in rovers:
        rovers[rid] = {}
    # Optionally store address
    if "address" in data:
        rovers[rid]["address"] = data.get("address")
    logger.info("Rover registered: %s", rid)
    render_screen()


def handle_telemetry_event(data: Any):
    """
    Handle TELEMETRY and TELEMETRY_UPDATE style events.
    Data may be:
      - {"rover_id": ..., "telemetry": {...}}
      - {...} (flat telemetry including rover_id and fields)
      - {...} (just telemetry fields without rover_id) - ignored if no id present
    """
    if not isinstance(data, dict):
        logger.debug("Telemetry event with non-dict data: %s", data)
        return

    # Case 1: wrapper with 'rover_id' + 'telemetry'
    if "rover_id" in data and "telemetry" in data and isinstance(data["telemetry"], dict):
        rid = data["rover_id"]
        telemetry = data["telemetry"]
        apply_telemetry(rid, telemetry)
        return

    # Case 2: flat telemetry including rover_id
    if "rover_id" in data and "position" in data:
        rid = data["rover_id"]
        apply_telemetry(rid, data)
        return

    # Case 3: telemetry_update style where data is telemetry without rover_id
    # try to recover rover_id from _maybe fields (not ideal)
    rid = data.get("rover_id") or data.get("_rover_id") or None
    if rid:
        apply_telemetry(rid, data)
        return

    # If no rover_id, check if event included an outer key mapping (rare), skip otherwise
    logger.debug("Received telemetry-like payload without rover_id: keys=%s", list(data.keys()))


async def ws_consumer():
    """Main websocket consumer loop with exponential backoff on connect errors."""
    connect_url = WS_URL
    backoff = 1.0  # seconds
    max_backoff = 10.0
    while True:
        try:
            logger.info("Connecting to %s", connect_url)
            async with websockets.connect(connect_url) as ws:
                logger.info("Connected to API WebSocket")
                backoff = 1.0  # reset backoff after success
                # render initial waiting screen
                render_screen()
                while True:
                    try:
                        msg = await ws.recv()
                    except websockets.ConnectionClosed:
                        logger.warning("WebSocket connection closed by server")
                        break
                    # Some servers may send simple PING text
                    if isinstance(msg, bytes):
                        try:
                            msg = msg.decode("utf-8")
                        except Exception:
                            logger.debug("Received non-utf8 bytes message")
                            continue

                    # Try parse JSON
                    parsed = None
                    try:
                        parsed = json.loads(msg)
                    except Exception:
                        # handle non-json text such as "PING"
                        txt = msg.strip()
                        if txt.upper() == "PING":
                            logger.debug("Received PING")
                            # no-op; keepalive
                            continue
                        logger.debug("Received non-JSON WS message: %s", repr(msg))
                        continue

                    # Expected shape: {"event": "...", "data": {...}, "ts": "..."}
                    ev = parsed.get("event") or parsed.get("type") or None
                    pdata = parsed.get("data", parsed)

                    logger.debug("Received event=%s data_keys=%s", ev, list(pdata.keys()) if isinstance(pdata, dict) else type(pdata))

                    if ev == "MISSION_SNAPSHOT":
                        # pdata should contain { "missions":..., "rovers": ... }
                        handle_mission_snapshot(pdata)
                    elif ev == "ROVER_REGISTERED":
                        handle_rover_registered(pdata)
                    elif ev in ("TELEMETRY", "TELEMETRY_UPDATE", "TELEMETRY_UPDATE_V1", "TELEMETRY_UPDATE_V2"):
                        # robustly handle different telemetry event shapes
                        handle_telemetry_event(pdata)
                    else:
                        # fallback: if data looks like telemetry, try to apply
                        if isinstance(pdata, dict) and ("position" in pdata or "battery_level_pct" in pdata):
                            # attempt to find rover_id field
                            if "rover_id" in pdata:
                                apply_telemetry(pdata["rover_id"], pdata)
                            else:
                                logger.debug("Received telemetry-like payload without explicit event: %s", pdata.keys())
                        else:
                            logger.debug("Unhandled WS event: %s", ev)
        except Exception as e:
            # Handle ConnectionRefused gracefully with longer backoff and no traceback spam
            if isinstance(e, ConnectionRefusedError) or "ConnectionRefusedError" in repr(e):
                logger.info("WebSocket connection refused by server — reconnecting in %.1fs", backoff)
            else:
                logger.exception("WebSocket error / reconnecting in %.1fs: %s", backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)
        # short delay before next loop iteration
        await asyncio.sleep(0.2)


def main():
    # Create and set an explicit event loop for this thread/process.
    # This avoids RuntimeError: "There is no current event loop in thread 'MainThread'."
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # allow Ctrl+C to stop gracefully
    def _on_sig(sig, frame):
        logger.info("Signal received, stopping")
        # Cancel all tasks running in this loop
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT, _on_sig)
    signal.signal(signal.SIGTERM, _on_sig)

    try:
        loop.run_until_complete(ws_consumer())
    except asyncio.CancelledError:
        logger.info("Stopped by user")
    except Exception as e:
        logger.exception("Unexpected error in main: %s", e)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()