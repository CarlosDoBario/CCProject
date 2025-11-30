import asyncio
import json
import logging
import threading
import uvicorn
import sys
from typing import Dict, Any, List, Optional, Callable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from common import config, utils
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore

logger = utils.get_logger("nave_mae.api")

# Variáveis globais para armazenar as instâncias dos Stores (preenchidas em startup.py)
mission_store: Optional[MissionStore] = None
telemetry_store: Optional[TelemetryStore] = None

# Fila assíncrona onde os hooks injetam eventos
event_queue: asyncio.Queue = asyncio.Queue()

# Conexões WebSocket ativas
active_connections: List[WebSocket] = []


def setup_stores(ms: MissionStore, ts: TelemetryStore):
    """ Função chamada pelo startup.py para injetar as instâncias de Store. """
    global mission_store, telemetry_store
    mission_store = ms
    telemetry_store = ts


# ----------------------------------------
# 1. API - Broadcast Hook (Integração)
# ----------------------------------------
def register_broadcast_hook():
    """
    Registra o hook nos stores para garantir que eventos sejam colocados na fila.
    Esta função deve ser chamada após a inicialização do loop.
    """
    if mission_store and telemetry_store:
        # A Nave-Mãe corre em asyncio. Criamos uma task assíncrona para lidar com eventos
        # que chegam de threads síncronas (ex: TelemetryStore).
        def sync_wrapper(event_type: str, payload: Dict[str, Any]):
            async def async_call():
                await broadcast_hook(event_type, payload)

            try:
                # Se houver um loop a correr, agenda a task
                loop = asyncio.get_running_loop()
                loop.create_task(async_call())
            except RuntimeError:
                # Se não houver loop (ex: chamado de uma thread síncrona), usamos o run_in_executor
                try:
                    loop = asyncio.get_event_loop()
                    loop.run_in_executor(None, lambda: loop.run_until_complete(async_call()))
                except Exception:
                    logger.warning("No running loop found to schedule broadcast hook.")

        # O TelemetryStore já está desenhado para agendar async hooks, então podemos passar a coroutine
        # diretamente para o MissionStore (que pode ter sync hooks)
        telemetry_store.register_hook(broadcast_hook)
        
        # O MissionStore (síncrono) também precisa de ser ligado ao broadcast
        mission_store.register_hook(sync_wrapper)

        logger.info("Broadcast hook registrado nos Stores.")


async def broadcast_hook(event_type: str, payload: Dict[str, Any]):
    """ Recebe o evento do Store e coloca-o na fila de broadcast. """
    try:
        # Normalizar o payload (garantir que o estado da missão vem como string)
        normalized_event = {
            "event": event_type.upper(),
            "ts": utils.now_iso(),
            "data": payload
        }
        await event_queue.put(normalized_event)
    except Exception:
        logger.exception("Falha ao colocar evento na fila de broadcast")


# ----------------------------------------
# 2. API - REST Endpoints
# ----------------------------------------
app = FastAPI(title="Nave-Mãe Observação API", version="1.0")


@app.get("/api/rovers", response_class=JSONResponse)
async def get_rovers_status():
    """ [REQUISITO MÍNIMO] Lista de rovers ativos, respetivo estado atual e última telemetria. """
    if mission_store is None or telemetry_store is None:
        return JSONResponse(status_code=503, content={"error": "Stores not initialized"})

    rovers = mission_store.list_rovers()
    output = []
    for rid, rdata in rovers.items():
        # Buscar último dado de telemetria
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
    """ [REQUISITO MÍNIMO] Lista de missões (ativas e concluídas), incluindo parâmetros principais. """
    if mission_store is None:
        return JSONResponse(status_code=503, content={"error": "MissionStore not initialized"})
    
    missions = mission_store.list_missions()
    # Retornar apenas os campos principais para a lista
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


# ----------------------------------------
# 3. API - WebSocket Endpoint (Tempo Real)
# ----------------------------------------
@app.websocket("/ws/subscribe")
async def websocket_endpoint(websocket: WebSocket):
    """ Canal de subscrição em tempo real para o Ground Control. """
    await websocket.accept()
    active_connections.append(websocket)
    logger.info("Novo Ground Control client conectado via WebSocket (%d ativos)", len(active_connections))
    
    # ⚠️ Enviar o estado inicial do MissionStore na conexão (opcional, mas robusto)
    try:
        if mission_store:
            snapshot = mission_store.snapshot()
            await websocket.send_json({"event": "MISSION_SNAPSHOT", "data": snapshot})
    except Exception:
        logger.warning("Falha ao enviar snapshot inicial.")

    # Loop de leitura da fila de eventos (Broadcast)
    while True:
        try:
            # Espera pelo próximo evento (timeout para detetar desconexão)
            event = await asyncio.wait_for(event_queue.get(), timeout=30) 
            await websocket.send_json(event)
            event_queue.task_done()
            
        except asyncio.TimeoutError:
            # Mantém a conexão viva enviando PING
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
    active_connections.remove(websocket)
    logger.info("Ground Control client desconectado (%d ativos)", len(active_connections))
    try:
        await websocket.close()
    except Exception:
        pass


# ----------------------------------------
# 4. Função de Lançamento (Chamada pelo startup.py)
# ----------------------------------------
def run_api_server(host: str = config.API_HOST, port: int = config.API_PORT):
    """ 
    Função de inicialização síncrona para ser executada em background (e.g., em thread) 
    pelo startup.py. 
    """
    logger.info("Iniciando API Server em http://%s:%d", host, port)
    
    # É preciso desativar o 'reload' e usar a App aqui definida
    # O log_level é passado via config.py
    uvicorn.run(app, host=host, port=port, log_level=config.LOG_LEVEL.lower(), access_log=False)