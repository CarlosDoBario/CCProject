#!/usr/bin/env python3
"""
API server for Nave‑Mãe: REST endpoints + WebSocket broadcast for Ground Control.

This file includes:
- snapshot sent on WS connect (missions + latest telemetry)
- resilient broadcast_hook that enqueues events and sends direct updates to connected WS clients
- compatibility: TELEMETRY -> TELEMETRY_UPDATE events to support older clients
- small post-snapshot resend delay to avoid race conditions where telemetry is
  processed at the same time as a client connection.
"""
import asyncio
import json
import logging
import threading
import sys
import time
from typing import Dict, Any, List, Optional, Callable, TYPE_CHECKING

from common import config, utils
logger = utils.get_logger("nave_mae.api")

# FastAPI conditional import
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # type: ignore
    from fastapi.responses import JSONResponse  # type: ignore
    _FASTAPI_AVAILABLE = True
except Exception:
    FastAPI = None  # type: ignore
    WebSocket = None  # type: ignore
    WebSocketDisconnect = Exception  # type: ignore
    JSONResponse = None  # type: ignore
    _FASTAPI_AVAILABLE = False
    logger.warning("FastAPI não disponível. Instala com: pip install fastapi[all]")

# For static type checking only
if TYPE_CHECKING:
    from fastapi import WebSocket  # type: ignore

# uvicorn conditional import
try:
    import uvicorn  # type: ignore
    _UVICORN_AVAILABLE = True
except Exception:
    uvicorn = None  # type: ignore
    _UVICORN_AVAILABLE = False
    logger.warning("uvicorn não disponível. Instala com: pip install uvicorn[standard]")

from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore

# FastAPI app instance (if available)
if _FASTAPI_AVAILABLE:
    app = FastAPI(title="Nave-Mãe Observação API", version="1.0")
else:
    app = None  # type: ignore

# Stores (injetados pelo startup script)
mission_store: Optional[MissionStore] = None
telemetry_store: Optional[TelemetryStore] = None

# Backwards-compatible event queue (kept for code that consumes it)
event_queue: asyncio.Queue = asyncio.Queue()

# Active WebSocket connections (forward reference so static checkers are happy)
active_connections: List["WebSocket"] = []  # type: ignore

# The event loop used by the API (captured on FastAPI startup)
api_loop: Optional[asyncio.AbstractEventLoop] = None


def setup_stores(ms: MissionStore, ts: TelemetryStore):
    """Inject the MissionStore and TelemetryStore instances."""
    global mission_store, telemetry_store
    mission_store = ms
    telemetry_store = ts


# Capture the api event loop on startup so we can schedule tasks there from other threads
if _FASTAPI_AVAILABLE:
    @app.on_event("startup")
    async def _api_on_startup():
        global api_loop
        api_loop = asyncio.get_running_loop()
        logger.info("API startup complete — api_loop captured")


# Safe non-blocking send helper
async def _safe_send(ws, event: Dict[str, Any]):
    try:
        await ws.send_json(event)
    except Exception:
        try:
            logger.debug("Failed to send event to websocket client (ignored)")
        except Exception:
            pass


# ----------------------------------------
# Broadcast hook registration & handler
# ----------------------------------------
def register_broadcast_hook():
    """
    Register wrappers for the stores so that broadcast_hook runs on the API loop.
    Must be called after setup_stores and after the api_loop is captured (if possible).
    """
    if mission_store is None or telemetry_store is None:
        logger.warning("register_broadcast_hook called before stores available")
        return

    def _schedule_broadcast(event_type: str, payload: Dict[str, Any]):
        # Prefer scheduling on api_loop if available (thread-safe)
        global api_loop
        if api_loop is not None:
            try:
                api_loop.call_soon_threadsafe(lambda: asyncio.create_task(broadcast_hook(event_type, payload)))
                return
            except Exception:
                logger.exception("Failed to schedule broadcast_hook on api_loop")

        # Fallbacks if no api_loop
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcast_hook(event_type, payload))
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
                loop.call_soon_threadsafe(lambda: asyncio.create_task(broadcast_hook(event_type, payload)))
            except Exception:
                logger.exception("No loop found to schedule broadcast_hook")

    def telemetry_sync_wrapper(event_type: str, payload: Dict[str, Any]):
        try:
            _schedule_broadcast(event_type, payload)
        except Exception:
            logger.exception("telemetry_sync_wrapper failed")

    def mission_sync_wrapper(event_type: str, payload: Dict[str, Any]):
        try:
            _schedule_broadcast(event_type, payload)
        except Exception:
            logger.exception("mission_sync_wrapper failed")

    # Attempt to register telemetry wrapper (TelemetryStore may expect sync or async hooks)
    try:
        telemetry_store.register_hook(telemetry_sync_wrapper)
    except Exception:
        try:
            telemetry_store.register_hook(broadcast_hook)
        except Exception:
            logger.exception("Falha ao registrar hook no telemetry_store")

    # Register mission store wrapper
    try:
        mission_store.register_hook(mission_sync_wrapper)
    except Exception:
        logger.exception("Falha ao registrar hook no mission_store")

    logger.info("Broadcast hook registrado nos Stores.")


async def broadcast_hook(event_type: str, payload: Dict[str, Any]):
    """
    Handle events coming from stores:
    - enqueue normalized event to event_queue (compatibility)
    - send immediately (best-effort) to connected WebSocket clients
    - if event is TELEMETRY, also emit TELEMETRY_UPDATE (flat) for client compatibility
    """
    try:
        logger.debug("broadcast_hook received event=%s payload_keys=%s",
                     event_type, list(payload.keys()) if isinstance(payload, dict) else type(payload))
    except Exception:
        logger.debug("broadcast_hook received event")

    normalized_event = {
        "event": event_type.upper(),
        "ts": utils.now_iso(),
        "data": payload
    }

    # 1) enqueue
    try:
        await event_queue.put(normalized_event)
    except Exception:
        logger.exception("Failed to put event into event_queue")

    # 2) direct send to active connections (best-effort)
    conns = list(active_connections)
    if conns:
        try:
            loop = asyncio.get_running_loop()
            for ws in conns:
                loop.create_task(_safe_send(ws, normalized_event))
        except Exception:
            logger.exception("Error scheduling direct websocket sends")

    # 3) compatibility: if TELEMETRY, also send TELEMETRY_UPDATE with flat telemetry object
    try:
        if normalized_event["event"] == "TELEMETRY":
            telemetry_obj = None
            if isinstance(payload, dict) and "telemetry" in payload and isinstance(payload["telemetry"], dict):
                telemetry_obj = payload["telemetry"]
            elif isinstance(payload, dict) and payload.get("rover_id") and "position" in payload:
                telemetry_obj = payload
            else:
                telemetry_obj = payload

            telemetry_update_event = {
                "event": "TELEMETRY_UPDATE",
                "ts": utils.now_iso(),
                "data": telemetry_obj
            }

            try:
                await event_queue.put(telemetry_update_event)
            except Exception:
                logger.exception("Failed to put TELEMETRY_UPDATE into event_queue")

            if conns:
                try:
                    loop = asyncio.get_running_loop()
                    for ws in conns:
                        loop.create_task(_safe_send(ws, telemetry_update_event))
                except Exception:
                    logger.exception("Error scheduling direct TELEMETRY_UPDATE sends")
    except Exception:
        logger.exception("Error during TELEMETRY->TELEMETRY_UPDATE compatibility step")


# ----------------------------------------
# REST & WebSocket endpoints
# ----------------------------------------
if _FASTAPI_AVAILABLE:
    @app.get("/api/rovers", response_class=JSONResponse)
    async def get_rovers_status():
        """Return list of rovers and latest telemetry."""
        if mission_store is None or telemetry_store is None:
            return JSONResponse(status_code=503, content={"error": "Stores not initialized"})
        rovers = mission_store.list_rovers()
        output = []
        for rid, rdata in rovers.items():
            latest_telemetry = telemetry_store.get_latest(rid) or {}
            output.append({
                "rover_id": rid,
                "state": rdata.get("state", "UNKNOWN"),
                "last_seen": rdata.get("last_seen"),
                "telemetry_latest": latest_telemetry,
            })
        return output


    @app.get("/api/missions", response_class=JSONResponse)
    async def get_missions_list():
        """Return missions summary list."""
        if mission_store is None:
            return JSONResponse(status_code=503, content={"error": "MissionStore not initialized"})
        missions = mission_store.list_missions()
        return [
            {
                "mission_id": m.get("mission_id"),
                "task": m.get("task"),
                "state": m.get("state"),
                "progress_pct": m.get("last_progress_pct", 0.0),
                "priority": m.get("priority"),
                "assigned_rover": m.get("assigned_rover"),
            }
            for m in missions.values()
        ]


    @app.websocket("/ws/subscribe")
    async def websocket_endpoint(websocket: WebSocket):
        """
        WebSocket endpoint for Ground Control clients.
        Sends:
         - MISSION_SNAPSHOT (missions + last telemetry for each rover)
         - immediate TELEMETRY_UPDATE events for rovers present at connect
         - a short delayed resend of TELEMETRY_UPDATE to mitigate race conditions
        Then listens to event_queue and forwards events.
        """
        await websocket.accept()
        active_connections.append(websocket)
        logger.info("Novo Ground Control client conectado via WebSocket (%d ativos)", len(active_connections))

        # Send combined snapshot (missions + latest telemetry)
        try:
            if mission_store:
                ms_snapshot = mission_store.snapshot() or {}
                missions_part = ms_snapshot.get("missions") if isinstance(ms_snapshot, dict) and "missions" in ms_snapshot else ms_snapshot

                rovers_part: Dict[str, Any] = {}
                try:
                    if telemetry_store:
                        for rid in telemetry_store.list_rovers():
                            latest = telemetry_store.get_latest(rid) or {}
                            rovers_part[rid] = latest
                except Exception:
                    logger.exception("Falha a recolher telemetria do TelemetryStore para snapshot inicial")

                combined_snapshot = {"missions": missions_part or {}, "rovers": rovers_part}
                await websocket.send_json({"event": "MISSION_SNAPSHOT", "data": combined_snapshot})

                # Send immediate TELEMETRY_UPDATE events for clients to apply
                try:
                    for rid, latest in rovers_part.items():
                        evt = {"event": "TELEMETRY_UPDATE", "data": latest}
                        await websocket.send_json(evt)
                        logger.debug("Sent initial TELEMETRY_UPDATE for %s to websocket client", rid)
                except Exception:
                    logger.exception("Falha ao enviar TELEMETRY_UPDATE iniciais ao cliente WebSocket")

                # --- Small delay and resend to avoid race conditions ---
                try:
                    await asyncio.sleep(0.05)  # short grace period for concurrent processing
                    rovers_after: Dict[str, Any] = {}
                    if telemetry_store:
                        for rid in telemetry_store.list_rovers():
                            latest = telemetry_store.get_latest(rid) or {}
                            rovers_after[rid] = latest

                    # Re-send updates (idempotent/harmless)
                    for rid, latest in rovers_after.items():
                        evt = {"event": "TELEMETRY_UPDATE", "data": latest}
                        await websocket.send_json(evt)
                        logger.debug("Re-sent TELEMETRY_UPDATE for %s after short delay", rid)
                except Exception:
                    logger.exception("Falha ao reenviar TELEMETRY_UPDATE pós-snapshot")
        except Exception:
            logger.warning("Falha ao enviar snapshot inicial.")

        # Main loop: forward events from event_queue to this websocket client
        while True:
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=30)
                try:
                    logger.debug("Sending event to websocket client: %s", event.get("event"))
                except Exception:
                    pass
                await websocket.send_json(event)
                event_queue.task_done()
            except asyncio.TimeoutError:
                # keepalive ping
                try:
                    await websocket.send_text("PING")
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
                logger.warning("Erro de WebSocket durante o envio; desconectando cliente.")
                break

        # Cleanup
        try:
            active_connections.remove(websocket)
        except Exception:
            pass
        logger.info("Ground Control client desconectado (%d ativos)", len(active_connections))
        try:
            await websocket.close()
        except Exception:
            pass


# Run helper for non-test entrypoints (kept for compatibility)
def run_api_server(host: str = config.API_HOST, port: int = config.API_PORT):
    logger.info("Iniciando API Server em http://%s:%d", host, port)

    if not _FASTAPI_AVAILABLE:
        raise RuntimeError("FastAPI não está instalado. Instala com: pip install fastapi[all]")

    if not _UVICORN_AVAILABLE:
        raise RuntimeError("uvicorn não está instalado. Instala com: pip install uvicorn[standard]")

    uvicorn.run(app, host=host, port=port, log_level=config.LOG_LEVEL.lower(), access_log=False)