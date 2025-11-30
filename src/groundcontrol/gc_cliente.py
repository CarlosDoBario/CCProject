import asyncio
import json
import os
from typing import Dict, Any, Optional
import websockets
import time

# Configs e Utilitários (assumindo importação do PYTHONPATH=src)
from common import config, utils

logger = utils.get_logger("gc.client")

# URL da API (usando configs)
WS_URL = f"ws://{config.TELEMETRY_HOST}:{config.API_PORT}/ws/subscribe"
API_PORT = config.API_PORT # Para logging

# --- Gerenciamento do Display e Estado ---
class GroundControlDisplay:
    def __init__(self):
        # Estado local (cópia da verdade central)
        self.rovers: Dict[str, Any] = {}
        self.missions: Dict[str, Dict[str, Any]] = {}
        self.status_message: str = "Aguardando conexão..."
        self.last_render_time: float = 0.0

    def _clear_and_print(self, content: str):
        # Limpar o ecrã (depende do SO)
        os.system('cls' if os.name == 'nt' else 'clear') 
        print(content)

    def render_cli(self, force: bool = False):
        """ Renderiza o estado no terminal, limitando o refresh rate. """
        
        now = time.time()
        if not force and (now - self.last_render_time < 0.5): # Limite a 2 FPS
            return
            
        self.last_render_time = now
        
        output = [
            "-" * 60,
            f"🛰️ GROUND CONTROL CLIENT - Status: {self.status_message}",
            "-" * 60
        ]
        
        # 1. VISUALIZAÇÃO DE MISSÕES E PROGRESSO
        output.append("### Missões Ativas / Pendentes:")
        
        missions_to_display = sorted(
            [m for m in self.missions.values() if m.get("state") not in ["COMPLETED", "CANCELLED"]], 
            key=lambda m: (-int(m.get('priority', 0)), m.get('mission_id', ''))
        )
        
        if not missions_to_display:
            output.append("  Nenhuma missão ativa ou pendente.")
            
        for m in missions_to_display:
            mid = m.get("mission_id", "N/A")
            state = m.get("state", "N/A")
            prog = m.get("last_progress_pct", 0.0)
            rover = m.get("assigned_rover", "N/A")
            
            output.append(f"  [{mid:<6}] Estado: {state:<12} | Rover: {rover:<6} | Progresso: {prog:>6.2f}%")

        output.append("-" * 60)

        # 2. VISUALIZAÇÃO DE ROVERS (TELEMETRIA E ESTADO)
        output.append("### Estado e Localização dos Rovers:")
        
        if not self.rovers:
            output.append("  Nenhum Rover online ou registado.")
            
        for rid, data in self.rovers.items():
            pos = data.get("position", {})
            batt = data.get("battery_level_pct", 0)
            status = data.get("status", "N/A").upper()
            x, y, z = pos.get("x", 0.0), pos.get("y", 0.0), pos.get("z", 0.0)
            
            # Formato de apresentação legível
            output.append(f"  {rid:<6} | STATUS: {status:<10} | POS: ({x:>7.2f}, {y:>7.2f}, {z:>5.2f}) | BATT: {batt:>5.1f}%")
            
        output.append("-" * 60)
        self._clear_and_print("\n".join(output))

    def handle_event(self, event: Dict[str, Any]):
        """ Processa e armazena o estado com base nos eventos WebSocket. """
        
        event_type = event.get("event")
        data = event.get("data", {})
        
        if event_type == "MISSION_SNAPSHOT" and data:
            # Recebe o estado inicial do MissionStore (snapshot completo)
            self.missions = data.get("missions", {})
            self.rovers = data.get("rovers", {}) # Inicializa com os rovers conhecidos
            self.status_message = "CONECTADO: Snapshot inicial carregado"

        elif event_type == "TELEMETRY_UPDATE" and data.get("rover_id"):
            rid = data["rover_id"]
            # Atualiza o estado da telemetria (posição, bateria)
            self.rovers[rid] = {**self.rovers.get(rid, {}), **data}
        
        elif event_type in ["MISSION_PROGRESS", "MISSION_ASSIGNED", "MISSION_COMPLETE", "MISSION_CANCEL", "MISSION_RECOVERED"]:
            # Eventos que afetam o estado da missão
            mid = data.get("mission_id")
            if mid and data.get("mission"): # Atualiza o objeto de missão
                m_update = data["mission"]
                self.missions[mid] = {**self.missions.get(mid, {}), **m_update}

            self.status_message = f"Evento de Missão: {mid} -> {event_type}"
            
        self.render_cli()

# --- Loop de Comunicação e Execução ---
async def listen_websocket(display: GroundControlDisplay):
    """ Tenta conectar e manter o loop de escuta WebSocket. """
    try:
        async with websockets.connect(WS_URL) as websocket:
            display.status_message = "CONECTADO: A receber streaming de eventos"
            display.render_cli(force=True)
            
            # Loop principal de escuta
            async for message in websocket:
                try:
                    event = json.loads(message)
                    display.handle_event(event)
                except json.JSONDecodeError:
                    logger.warning("Mensagem JSON inválida recebida.")
                except Exception as e:
                    logger.exception("Falha ao processar evento.")
                    
    except ConnectionRefusedError:
        display.status_message = f"FALHA DE CONEXÃO: Nave-Mãe offline em {API_PORT}."
        logger.error(display.status_message)
    except websockets.exceptions.ConnectionClosed:
        display.status_message = "DESCONECTADO: Conexão fechada."
    except Exception as e:
        display.status_message = f"ERRO CRÍTICO: {e}"
        logger.exception(display.status_message)


async def main_async():
    display = GroundControlDisplay()
    
    # Loop de reconexão
    while True:
        await listen_websocket(display)
        await asyncio.sleep(5) 
        

if __name__ == "__main__":
    # Certifique-se de que a Nave-Mãe (ml_server + telemetry_server + api_server) está a correr primeiro
    try:
        asyncio.run(main_async()) 
    except KeyboardInterrupt:
        logger.info("Ground Control encerrado pelo usuário.")