import asyncio
import json
import logging
import sys
import threading
import time
from typing import Dict, Any, List, Optional, Tuple, Callable

# Importações FastAPI/Uvicorn
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect # type: ignore
    from fastapi.responses import JSONResponse # type: ignore
    import uvicorn # type: ignore
    _FASTAPI_AVAILABLE = True
except ImportError:
    logging.error("FastAPI/Uvicorn não estão instalados. Instala com: pip install fastapi[all] uvicorn")
    _FASTAPI_AVAILABLE = False
    
from common import config, utils
from nave_mae.mission_store import MissionStore
from nave_mae.telemetry_store import TelemetryStore

logger = utils.get_logger("nave_mae.api_server")

# --- Instâncias (Injetadas) ---
app = FastAPI(title="Nave-Mãe Observação API", version="1.0")
mission_store: Optional[MissionStore] = None
telemetry_store: Optional[TelemetryStore] = None

# --- Helpers de Inicialização ---

def setup_stores(ms: MissionStore, ts: TelemetryStore):
    """Injeta as instâncias do MissionStore e TelemetryStore."""
    global mission_store, telemetry_store
    mission_store = ms
    telemetry_store = ts
    logger.info("Stores de Missão e Telemetria injetados no API Server.")

# --- Endpoints REST (Requisito do Enunciado) ---

def _force_reload_mission_store():
    """Força o recarregamento do MissionStore do disco para obter o estado atual."""
    try:
        if mission_store and hasattr(mission_store, 'load_from_file'):
            mission_store.load_from_file()
    except Exception:
        logger.warning("Falha ao recarregar MissionStore para API request.")


def _get_detailed_telemetry_from_rover_data(rdata: Dict[str, Any]) -> Dict[str, Any]:
    """Extrai os campos numéricos detalhados do registo do rover."""
    
    # 1. Tenta usar o payload completo que o ML Server salvou (chave "last_telemetry_full")
    full_payload = rdata.get('last_telemetry_full', {})
    
    # 2. Faz fallback para os campos simples no nível superior do registo do rover (que o ML Server também atualiza)
    return {
        "battery_level_pct": full_payload.get("battery_level_pct", rdata.get("battery_level_pct", 0.0)),
        "internal_temp_c": full_payload.get("internal_temp_c", rdata.get("internal_temp_c", 0.0)),
        "current_speed_m_s": full_payload.get("current_speed_m_s", rdata.get("current_speed_m_s", 0.0)),
        "position": full_payload.get("position", rdata.get("position", {"x": 0.0, "y": 0.0, "z": 0.0})),
        "status": full_payload.get("status", rdata.get("state", "UNKNOWN")),
        "timestamp_ms": full_payload.get("timestamp_ms", utils.now_ms()),
    }


@app.get("/api/missions", response_class=JSONResponse)
async def get_missions_list():
    """Retorna a lista de missões (ativas e concluídas) com o progresso mais recente."""
    _force_reload_mission_store()
    
    if mission_store is None:
        return JSONResponse(status_code=503, content={"error": "MissionStore not initialized"})
    
    missions = mission_store.list_missions()
    output = []
    
    for m in missions.values():
        output.append({
            "mission_id": m.get("mission_id"),
            "task": m.get("task"),
            "state": m.get("state"),
            "progress_pct": m.get("last_progress_pct", 0.0), 
            "priority": m.get("priority"),
            "assigned_rover": m.get("assigned_rover"),
        })
    return output


@app.get("/api/rovers", response_class=JSONResponse)
async def get_rovers_status():
    """Retorna a lista de rovers e o seu estado atual (incluindo telemetria customizada)."""
    _force_reload_mission_store()

    if mission_store is None or telemetry_store is None:
        return JSONResponse(status_code=503, content={"error": "Stores not initialized"})
    
    rovers_ms = mission_store.list_rovers()
    output = {}
    
    for rid, rdata in rovers_ms.items():
        # Extrair dados detalhados usando a nova lógica
        detailed_data = _get_detailed_telemetry_from_rover_data(rdata)
        
        output[rid] = {
            "rover_id": rid,
            "state": rdata.get("state", "UNKNOWN"), 
            "last_seen": rdata.get("last_seen"),
            
            # Dados detalhados que o Ground Control precisa
            "battery_level_pct": detailed_data["battery_level_pct"],
            "internal_temp_c": detailed_data["internal_temp_c"],
            "current_speed_m_s": detailed_data["current_speed_m_s"],
            "position": detailed_data["position"],
        }
    return output


@app.get("/api/telemetry/latest", response_class=JSONResponse)
async def get_latest_telemetry_all():
    """Retorna os últimos dados de telemetria detalhados de todos os rovers."""
    _force_reload_mission_store()

    if mission_store is None:
        return JSONResponse(status_code=503, content={"error": "MissionStore not initialized"})
    
    rovers_ms = mission_store.list_rovers()
    latest_data = {}
    
    for rid, rdata in rovers_ms.items():
        # A Telemetria mais "fresca" está no registo do rover do MissionStore
        detailed_data = _get_detailed_telemetry_from_rover_data(rdata)
        
        latest_data[rid] = {
            "status": detailed_data["status"],
            "battery_level_pct": detailed_data["battery_level_pct"],
            "internal_temp_c": detailed_data["internal_temp_c"],
            "current_speed_m_s": detailed_data["current_speed_m_s"],
            "position": detailed_data["position"],
            "timestamp_ms": detailed_data["timestamp_ms"],
        }
    
    return {"latest_telemetry": latest_data}


# --- Lógica de Execução com Uvicorn ---

def run_api_server_uvicorn():
    """Inicia o servidor API usando Uvicorn."""
    if not _FASTAPI_AVAILABLE:
        sys.exit(1)
        
    logger.info("Iniciando API Server (Uvicorn) em http://%s:%d", config.API_HOST, config.API_PORT)

    uvicorn.run(
        app, 
        host=config.API_HOST, 
        port=config.API_PORT, 
        log_level=config.LOG_LEVEL.lower(), 
        access_log=False 
    )


# --- Bloco Principal (Entrypoint) ---
if __name__ == "__main__":
    # 1. Configuração de Logging 
    import logging
    try:
        config.configure_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # 2. Inicialização dos Stores e Injeção
    class MockStore:
        def list_missions(self): return {"M-0001": {"mission_id": "M-0001", "state": "CREATED", "task": "capture_images", "priority": 1, "last_progress_pct": 0.0, "history": []}}
        def list_rovers(self): 
            # Mock de dados detalhados para o teste da API
            return {"R-TEST": {"state": "IDLE", "last_seen": utils.now_iso(), "battery_level_pct": 100, "internal_temp_c": 25.0, "current_speed_m_s": 0.0}}
        def get_rover(self, rid): return self.list_rovers().get(rid)
        def load_from_file(self): pass 
    
    try:
        from nave_mae.telemetry_store import TelemetryStore
        from nave_mae.mission_store import MissionStore
        ms = MissionStore(persist_file=config.MISSION_STORE_FILE)
        ts = TelemetryStore(mission_store=ms)
    except Exception:
        logger.error("Could not load real stores. Using Mock API data.")
        ms = MockStore()
        ts = MockStore()

    # 3. Injeção e Inicialização
    setup_stores(ms, ts)
    
    # Executar o Servidor API
    run_api_server_uvicorn()