#!/usr/bin/env python3
"""
Ground Control client (console) - robust WebSocket consumer that applies
MISSION_SNAPSHOT, ROVER_REGISTERED, TELEMETRY, TELEMETRY_UPDATE and mission events
immediately on first receipt and falls back to periodic API polling.

Updated behaviour:
- Show COMPLETED missions for a short grace period, then remove them from the active list.
- Suppress re-adding the same completed mission for a configurable suppression window,
  so they don't keep reappearing if the API still returns completed items.
- Config via env:
    GC_COMPLETED_GRACE (seconds, default 3.0)
    GC_SUPPRESSION_WINDOW (seconds, default 60.0)
    GC_API_POLL_INTERVAL (seconds, default 1.0)
"""
from __future__ import annotations
import asyncio
import json
import os
import logging
import signal
import sys
import traceback
import time
from typing import Dict, Any, Optional

from common import config

try:
    import websockets
except Exception:
    print("Instala a dependência 'websockets' (pip install websockets) e tenta de novo.")
    raise

try:
    import requests
except Exception:
    print("Instala a dependência 'requests' (pip install requests) e tenta de novo.")
    raise

# Configure logging from env
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s [gc_cliente] %(message)s")
logger = logging.getLogger("gc_cliente")

WS_HOST = "127.0.0.1" if config.API_HOST == "0.0.0.0" else config.API_HOST
WS_URL = f"ws://{WS_HOST}:{config.API_PORT}/ws/subscribe"
API_HOST = WS_HOST
API_BASE = f"http://{API_HOST}:{config.API_PORT}"

# Internal state
rovers: Dict[str, Dict[str, Any]] = {}     # rover_id -> telemetry/state dict
missions: Dict[str, Any] = {}              # mission_id -> mission data (augmented)
missions_lock = asyncio.Lock()
# completed tracking: mission_id -> timestamp when first observed as COMPLETED
completed_seen: Dict[str, float] = {}
# removed completed suppression: mission_id -> timestamp when removed
removed_completed: Dict[str, float] = {}

# Configurable timings
COMPLETED_GRACE = float(os.environ.get("GC_COMPLETED_GRACE", "3.0"))
SUPPRESSION_WINDOW = float(os.environ.get("GC_SUPPRESSION_WINDOW", "60.0"))
API_POLL_INTERVAL = float(os.environ.get("GC_API_POLL_INTERVAL", "1.0"))


def clear_screen():
    try:
        if os.name == "nt":
            os.system("cls")
        else:
            sys.stdout.write("\x1b[2J\x1b[H")
    except Exception:
        pass


def render_screen():
    """Console rendering of missions and rovers state (thread-safe caller assumed)."""
    clear_screen()
    print("-" * 60)
    print("🛰️  GROUND CONTROL CLIENT - Status: CONECTADO")
    print("-" * 60)
    # Missions
    print("### Missões Ativas / Pendentes:")
    if not missions:
        print("  Nenhuma missão ativa ou pendente.")
    else:
        # sort missions by mission_id (or priority if present)
        ms = sorted(missions.values(), key=lambda m: m.get("mission_id") or "")
        for m in ms:
            mid = m.get("mission_id") or "<unknown>"
            task = m.get("task") or ""
            state = str(m.get("state") or "UNKNOWN").upper()
            prog = float(m.get("progress_pct") or m.get("progress") or 0.0)
            assigned = m.get("assigned_rover") or m.get("rover") or ""
            if state == "COMPLETED":
                label = f"{mid} | {task} | STATE: COMPLETED | PROG: 100.0%"
            else:
                label = f"{mid} | {task} | STATE: {state} | PROG: {prog:.1f}%"
            if assigned:
                label += f" | ROVER: {assigned}"
            print("  " + label)
    print("-" * 60)
    # Rovers (best-effort)
    print("### Estado e Localização dos Rovers:")
    if not rovers:
        print("  Nenhum Rover online ou registado.")
    else:
        for rid, data in rovers.items():
            pos = data.get("position") or {}
            x = pos.get("x", 0.0) if isinstance(pos, dict) else 0.0
            y = pos.get("y", 0.0) if isinstance(pos, dict) else 0.0
            z = pos.get("z", 0.0) if isinstance(pos, dict) else 0.0
            batt = data.get("battery_level_pct", 0.0) if isinstance(data.get("battery_level_pct", 0.0), (int, float)) else 0.0
            prog = data.get("progress_pct", 0.0) if isinstance(data.get("progress_pct", 0.0), (int, float)) else 0.0
            status = data.get("status", "N/A")
            try:
                print(f"  {rid} | STATUS: {status:7} | POS: ({x:6.2f}, {y:6.2f}, {z:5.2f}) | BATT: {batt:6.1f}% | PROG: {prog:6.1f}%")
            except Exception:
                print(f"  {rid} | STATUS: {status} | POS: {pos} | BATT: {batt} | PROG: {prog}")
    print("-" * 60)
    print("Última atualização:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    sys.stdout.flush()


def apply_telemetry(rover_id: str, telemetry: Dict[str, Any]):
    """Merge telemetry into in-memory rover state and trigger render."""
    if not rover_id:
        return
    cur = rovers.setdefault(rover_id, {})
    if isinstance(telemetry, dict):
        cur.update(telemetry)
    cur["rover_id"] = rover_id
    # Also, if telemetry contains mission progress info, reflect on missions mapping
    prog = telemetry.get("progress_pct") or telemetry.get("progress")
    mid = telemetry.get("mission_id") or telemetry.get("mission")
    if mid:
        # update mission entry (async)
        asyncio.get_event_loop().create_task(update_mission_from_event({"mission_id": mid, "progress_pct": prog, "assigned_rover": rover_id}))
    render_screen()


async def update_mission_from_event(pdata: Dict[str, Any]):
    """
    Update internal missions mapping from a partial mission payload (pdata).
    pdata may contain mission_id, state, progress_pct, assigned_rover, task, etc.
    This function respects the removed_completed suppression window.
    """
    mid = pdata.get("mission_id") or pdata.get("id") or pdata.get("mission")
    if not mid:
        return
    now = time.time()
    # If this mission was recently removed as completed, suppress re-adding until suppression window expires
    removed_ts = removed_completed.get(mid)
    if removed_ts and (now - removed_ts) < SUPPRESSION_WINDOW:
        logger.debug("Suppressing re-add of recently removed completed mission %s (%.1fs left)", mid, SUPPRESSION_WINDOW - (now - removed_ts))
        return

    async with missions_lock:
        m = missions.get(mid, {})
        # merge sensible keys
        if "task" in pdata and pdata.get("task"):
            m["task"] = pdata.get("task")
        if "state" in pdata and pdata.get("state"):
            m["state"] = str(pdata.get("state")).upper()
        if pdata.get("progress_pct") is not None:
            try:
                m["progress_pct"] = float(pdata.get("progress_pct"))
            except Exception:
                pass
        elif pdata.get("progress") is not None:
            try:
                m["progress_pct"] = float(pdata.get("progress"))
            except Exception:
                pass
        # assigned rover
        if pdata.get("assigned_rover"):
            m["assigned_rover"] = pdata.get("assigned_rover")
        if pdata.get("rover"):
            m["assigned_rover"] = pdata.get("rover")
        # mission id and raw payload
        m["mission_id"] = mid
        m["raw"] = pdata
        missions[mid] = m

        # track completion time
        st = m.get("state", "").upper()
        if st == "COMPLETED" or (float(m.get("progress_pct", 0.0) or 0.0) >= 100.0):
            if mid not in completed_seen:
                completed_seen[mid] = now
        else:
            # if progress dropped below 100 again, clear completed tracking
            completed_seen.pop(mid, None)
    render_screen()


async def poll_api_missions():
    """Periodic fallback polling to /api/missions to refresh authoritative mission list."""
    session = requests.Session()
    url = f"{API_BASE}/api/missions"
    while True:
        try:
            r = session.get(url, timeout=2.0)
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = None
                # Accept list or {"missions": [...]}
                list_missions = []
                if isinstance(data, dict) and "missions" in data and isinstance(data["missions"], list):
                    list_missions = data["missions"]
                elif isinstance(data, list):
                    list_missions = data
                elif isinstance(data, dict):
                    # pick first list value
                    for v in data.values():
                        if isinstance(v, list):
                            list_missions = v
                            break
                # update internal mapping
                async with missions_lock:
                    now = time.time()
                    seen = set()
                    for raw in list_missions:
                        # try to extract mission id from common fields
                        mid = raw.get("mission_id") or raw.get("id") or raw.get("name")
                        if not mid:
                            continue
                        # If mission was recently removed after completion, suppress re-adding within suppression window
                        removed_ts = removed_completed.get(mid)
                        if removed_ts and (now - removed_ts) < SUPPRESSION_WINDOW:
                            logger.debug("Skipping API-returned completed mission %s due to suppression (%.1fs left)", mid, SUPPRESSION_WINDOW - (now - removed_ts))
                            continue

                        seen.add(mid)
                        m = missions.get(mid, {})
                        # map common fields
                        m["mission_id"] = mid
                        m["task"] = raw.get("task") or raw.get("type") or m.get("task")
                        state = raw.get("state") or raw.get("mission_state") or m.get("state")
                        if state:
                            m["state"] = str(state).upper()
                        # progress fallback
                        prog = raw.get("progress_pct") or raw.get("progress") or 0.0
                        try:
                            m["progress_pct"] = float(prog)
                        except Exception:
                            pass
                        # assigned rover
                        assigned = raw.get("assigned_rover") or raw.get("rover_id") or raw.get("assigned_to")
                        if assigned:
                            m["assigned_rover"] = assigned
                        m["raw"] = raw
                        missions[mid] = m
                        # if completed, register timestamp (for grace)
                        if m.get("state") == "COMPLETED" or float(m.get("progress_pct", 0.0) or 0.0) >= 100.0:
                            if mid not in completed_seen:
                                completed_seen[mid] = now

                    # purge missions that disappeared and are not recently completed
                    for exist_mid in list(missions.keys()):
                        if exist_mid not in seen and exist_mid not in completed_seen:
                            missions.pop(exist_mid, None)

                    # remove completed after grace and move to removed_completed suppression
                    now = time.time()
                    to_remove = []
                    for mid, ts in list(completed_seen.items()):
                        if now - ts > COMPLETED_GRACE:
                            to_remove.append(mid)
                    for mid in to_remove:
                        completed_seen.pop(mid, None)
                        # move to removed_completed to suppress re-adds for SUPPRESSION_WINDOW
                        removed_completed[mid] = time.time()
                        missions.pop(mid, None)
                render_screen()
            else:
                # non-200, ignore for now
                logger.debug("API poll returned status %s", r.status_code)
        except Exception as e:
            logger.debug("API poll error: %s", e)
        await asyncio.sleep(API_POLL_INTERVAL)


async def handle_ws_message(msg: str):
    """Process a single WS message string (JSON or plain text)."""
    if not msg:
        return
    # decode bytes if needed
    if isinstance(msg, bytes):
        try:
            msg = msg.decode("utf-8")
        except Exception:
            return
    # ignore ping texts
    if msg.strip().upper() == "PING":
        return
    try:
        parsed = json.loads(msg)
    except Exception:
        logger.debug("Non-JSON WS message: %s", msg)
        return

    ev = parsed.get("event") or parsed.get("type") or parsed.get("evt")
    pdata = parsed.get("data") if isinstance(parsed.get("data"), dict) else parsed.get("data", parsed)

    # handle mission snapshot (full replacement)
    if ev == "MISSION_SNAPSHOT":
        try:
            ms = pdata.get("missions", {}) if isinstance(pdata, dict) else {}
            rovers_block = pdata.get("rovers", {}) if isinstance(pdata, dict) else {}
            async with missions_lock:
                missions.clear()
                completed_seen.clear()
                removed_completed.clear()
                if isinstance(ms, dict):
                    for k, v in ms.items():
                        missions[k] = v if isinstance(v, dict) else {"mission_id": k, "raw": v}
                elif isinstance(ms, list):
                    for raw in ms:
                        mid = raw.get("mission_id") or raw.get("id")
                        if mid:
                            missions[mid] = raw
                # apply rovers block into telemetry
                for rid, t in (rovers_block or {}).items():
                    if isinstance(t, dict):
                        apply_telemetry(rid, t)
        except Exception:
            logger.exception("Error handling MISSION_SNAPSHOT")
        render_screen()
        return

    # handle explicit rover registration
    if ev == "ROVER_REGISTERED":
        try:
            rid = pdata.get("rover_id") or pdata.get("id")
            if rid:
                if rid not in rovers:
                    rovers[rid] = {}
                if "address" in pdata:
                    rovers[rid]["address"] = pdata.get("address")
        except Exception:
            logger.exception("Error handling ROVER_REGISTERED")
        render_screen()
        return

    # telemetry events
    if ev and ev.upper() in ("TELEMETRY", "TELEMETRY_UPDATE", "TELEMETRY_UPDATE_V1", "TELEMETRY_UPDATE_V2"):
        # robustly accept wrapper or flat telemetry
        try:
            if isinstance(pdata, dict) and "rover_id" in pdata and "telemetry" in pdata and isinstance(pdata["telemetry"], dict):
                apply_telemetry(pdata["rover_id"], pdata["telemetry"])
            elif isinstance(pdata, dict) and "rover_id" in pdata:
                apply_telemetry(pdata["rover_id"], pdata)
            else:
                # sometimes the server sends flat telemetry (without wrapper)
                for key in ("rover_id", "id"):
                    if isinstance(pdata, dict) and key in pdata:
                        apply_telemetry(pdata.get(key), pdata)
                        break
        except Exception:
            logger.exception("Error handling telemetry WS event")
        return

    # mission-related events: try to update mission mapping from pdata
    # Accept events like MISSION_ASSIGN, MISSION_PROGRESS, MISSION_COMPLETE, MISSION_UPDATED, MISSION_CREATED
    if ev and ev.upper().startswith("MISSION"):
        try:
            # If the mission was recently removed as completed, suppress re-add until suppression window expires
            mid = pdata.get("mission_id") or pdata.get("id") or pdata.get("mission")
            if mid:
                removed_ts = removed_completed.get(mid)
                if removed_ts and (time.time() - removed_ts) < SUPPRESSION_WINDOW:
                    logger.debug("Suppressing WS mission event for recently removed completed mission %s", mid)
                    return
            await update_mission_from_event(pdata if isinstance(pdata, dict) else {"mission_id": None})
        except Exception:
            logger.exception("Error handling mission WS event")
        return

    # Fallback: if payload looks like mission-like (has mission_id or progress)
    if isinstance(pdata, dict) and ("mission_id" in pdata or "progress_pct" in pdata or "progress" in pdata):
        # check suppression first
        mid = pdata.get("mission_id") or pdata.get("id") or pdata.get("mission")
        if mid and (mid in removed_completed) and ((time.time() - removed_completed[mid]) < SUPPRESSION_WINDOW):
            logger.debug("Suppressing fallback WS mission-like payload for %s due to suppression", mid)
            return
        asyncio.get_event_loop().create_task(update_mission_from_event(pdata))
        return

    # Unknown event: ignore but log debug
    logger.debug("Unhandled WS event=%s keys=%s", ev, list(pdata.keys()) if isinstance(pdata, dict) else type(pdata))


async def ws_consumer():
    """Main websocket consumer with reconnection/backoff."""
    connect_url = WS_URL
    backoff = 1.0
    max_backoff = 10.0
    while True:
        try:
            logger.info("Connecting to %s", connect_url)
            async with websockets.connect(connect_url) as ws:
                logger.info("Connected to API WebSocket")
                # initial render
                render_screen()
                while True:
                    try:
                        msg = await ws.recv()
                    except websockets.ConnectionClosed:
                        logger.warning("WebSocket connection closed by server")
                        break
                    # process message
                    try:
                        await handle_ws_message(msg)
                    except Exception:
                        logger.exception("Error processing WS message")
        except Exception as e:
            logger.warning("WebSocket error/reconnect in %.1fs: %s", backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)


def shutdown(loop):
    logger.info("Shutting down GC client")
    for task in asyncio.all_tasks(loop):
        task.cancel()


def main():
    # explicit event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # graceful shutdown handlers
    def on_sig(sig, frame):
        logger.info("Signal received, stopping")
        shutdown(loop)

    signal.signal(signal.SIGINT, on_sig)
    signal.signal(signal.SIGTERM, on_sig)

    try:
        # schedule ws consumer and api poller
        tasks = [
            loop.create_task(ws_consumer()),
            loop.create_task(poll_api_missions()),
        ]
        loop.run_until_complete(asyncio.gather(*tasks))
    except asyncio.CancelledError:
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Unexpected error in main")
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