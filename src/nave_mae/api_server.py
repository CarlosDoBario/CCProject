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
import traceback
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

# Backwards-compatible event queue (initialized on startup to bind to the api event loop)
event_queue: Optional[asyncio.Queue] = None

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
        global api_loop, event_queue
        api_loop = asyncio.get_running_loop()
        # initialize event_queue here so it is bound to the running loop
        event_queue = asyncio.Queue()
        logger.info("API startup complete — api_loop captured and event_queue initialized")


# Helper: robust JSON serialization (fall back to str for unknown types)
def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj)
    except Exception:
        try:
            return json.dumps(obj, default=str)
        except Exception:
            try:
                return json.dumps({"error": "serialization_failed", "repr": repr(obj)})
            except Exception:
                return '{"error":"serialization_failed","repr":"<unserializable>"}'


# Safe non-blocking send helper
async def _safe_send(ws, event: Dict[str, Any]) -> bool:
    try:
        payload = _json_dumps_safe(event)
        await ws.send_text(payload)
        return True
    except Exception:
        traceback.print_exc()
        try:
            logger.exception("Failed to send event to websocket client")
        except Exception:
            pass
        return False


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

    # 1) enqueue (only if event_queue initialized)
    global event_queue
    try:
        if event_queue is not None:
            await event_queue.put(normalized_event)
        else:
            logger.debug("broadcast_hook: event_queue not initialized; skipping enqueue")
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
                if event_queue is not None:
                    await event_queue.put(telemetry_update_event)
                else:
                    logger.debug("broadcast_hook: event_queue not initialized; skipping TELEMETRY_UPDATE enqueue")
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
        logger.info("Novo Ground Control cliente conectado via WebSocket (%d ativos)", len(active_connections))

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
                ok = await _safe_send(websocket, {"event": "MISSION_SNAPSHOT", "data": combined_snapshot})
                if not ok:
                    logger.warning("Failed to deliver initial MISSION_SNAPSHOT (client might disconnect)")

                # Send immediate TELEMETRY_UPDATE events for clients to apply
                try:
                    for rid, latest in rovers_part.items():
                        evt = {"event": "TELEMETRY_UPDATE", "data": latest}
                        ok = await _safe_send(websocket, evt)
                        if ok:
                            logger.debug("Sent initial TELEMETRY_UPDATE for %s to websocket client", rid)
                        else:
                            logger.warning("Failed to send initial TELEMETRY_UPDATE for %s", rid)
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
                        ok = await _safe_send(websocket, evt)
                        if ok:
                            logger.debug("Re-sent TELEMETRY_UPDATE for %s after short delay", rid)
                        else:
                            logger.warning("Failed to re-send TELEMETRY_UPDATE for %s", rid)
                except Exception:
                    logger.exception("Falha ao reenviar TELEMETRY_UPDATE pós-snapshot")
        except Exception:
            # improved logging for the outer try so we capture full traceback
            logger.exception("Falha ao enviar snapshot inicial.")

        # Main loop: forward events from event_queue to this websocket client
        while True:
            try:
                # Ensure event_queue is available (should be initialized on startup)
                global event_queue
                if event_queue is None:
                    # graceful sleep and continue if queue not ready
                    await asyncio.sleep(0.2)
                    continue

                event = await asyncio.wait_for(event_queue.get(), timeout=30)
                try:
                    logger.debug("Sending event to websocket client: %s", event.get("event"))
                except Exception:
                    pass
                sent = await _safe_send(websocket, event)
                event_queue.task_done()
                if not sent:
                    logger.warning("Send to websocket client failed; breaking out of forward loop")
                    break
            except asyncio.TimeoutError:
                # keepalive ping
                try:
                    await websocket.send_text("PING")
                except Exception:
                    break
            except WebSocketDisconnect:
                break
            except Exception:
                # log full exception here for easier debugging
                logger.exception("Erro de WebSocket durante o envio; desconectando cliente.")
                break

        # Cleanup
        try:
            if websocket in active_connections:
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