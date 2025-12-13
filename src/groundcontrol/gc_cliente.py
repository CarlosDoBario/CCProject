import argparse
import time
import json
import logging
from typing import Dict, Any, List, Optional
import requests  # em comentario para nao dar erro de import. 

from common import config, utils

logger = utils.get_logger("groundcontrol.client")


class GroundControlClient:
    # Definição estática dos postos de carregamento (deve corresponder ao RoverSim)
    CHARGING_STATIONS = [
        {"id": "CS-A", "x": 45.0, "y": 45.0, "z": 0.0},
        {"id": "CS-B", "x": 24.0, "y": 24.0, "z": 0.0}
    ]

    def __init__(self, api_base_url: str, polling_interval: float = 5.0):
        self.api_base_url = api_base_url.rstrip('/')
        self.polling_interval = polling_interval
        logger.info(f"Ground Control Client started. API: {self.api_base_url}")
        
    def _fetch_api(self, endpoint: str) -> Optional[Any]:
        """Tenta fazer um GET request à API."""
        url = f"{self.api_base_url}{endpoint}"
        try:
            response = requests.get(url, timeout=self.polling_interval - 1.0)
            response.raise_for_status()
            # Retorna o conteúdo decodificado (pode ser dict ou list)
            return response.json()
        except requests.exceptions.Timeout:
            logger.warning(f"API request timed out: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API connection error ({url}): {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON response from {url}")
            return None

    def _format_charging_stations(self) -> str:
        """Formata a lista estática de postos de carregamento para a dashboard."""
        output = ["\n--- POSTOS DE CARREGAMENTO ---"]
        
        for station in self.CHARGING_STATIONS:
            line = (
                f"  - Posto ID: {station['id']} | "
                f"Posição (X,Y,Z): ({station['x']:.1f}, {station['y']:.1f}, {station['z']:.1f})"
            )
            output.append(line)
        return "\n".join(output)
        
    def _format_rovers(self, rovers_data: Dict[str, Dict[str, Any]]) -> str:
        """
        Formata o estado dos rovers, consolidando toda a telemetria detalhada 
        (Posição X, Y, Z, Velocidade, Bateria e Temperatura) nesta secção.
        """
        if not rovers_data:
            return "   [Nenhum Rover Ativo]\n"
            
        output = ["\n--- ESTADO DOS ROVERS ---"]
        for rid, status in rovers_data.items():
            batt = status.get('battery_level_pct', 0.0)
            temp = status.get('internal_temp_c', 0.0)
            speed = status.get('current_speed_m_s', 0.0)
            
            # Posição, obtida diretamente do payload do API Server 
            pos = status.get('position', {"x": 0.0, "y": 0.0, "z": 0.0})
            
            # Lógica de Estado 
            state_raw = status.get('status', 'DESCONHECIDO').upper() 
            if state_raw in ('CHARGING_TRAVEL', 'TRAVELING_TO_CHARGE', 'MOVING_TO_MISSION', 'MOVING'):
                display_state = 'MOVING'
            elif state_raw in ('IN_MISSION', 'RUNNING'):
                display_state = 'IN_MISSION'
            elif state_raw == 'COOLING_DOWN':
                display_state = 'COOLING'
            elif state_raw == 'CHARGING':
                display_state = 'CHARGING'
            else:
                # IDLE para COMPLETED, IDLE, ERROR, UNKNOWN
                display_state = 'IDLE' 
            
            # 1. Linha principal do Estado (Bateria e Temperatura)
            line = (
                f"  - Rover ID: {rid} | ESTADO: {display_state} | Bateria: {batt:.1f}% | Temperatura: {temp:.1f}°C"
            )
            # 2. Linha de Telemetria Detalhada (Posição XYZ e Velocidade)
            telemetry_line = (
                f"    -> Posição (X,Y,Z): ({pos.get('x', 0.0):.1f}, {pos.get('y', 0.0):.1f}, {pos.get('z', 0.0):.1f}) | "
                f"Velocidade: {speed:.1f} m/s"
            )
            
            output.append(line)
            output.append(telemetry_line)

        return "\n".join(output)

    def _format_missions(self, missions_list: List[Dict[str, Any]]) -> str:
        """Formata a lista de missões, incluindo a área (XYZ) e duração."""
        if not missions_list:
            return "   [Nenhuma Missão Criada]\n"
            
        output = ["\n--- MISSÕES (Área & Duração) ---"]
        
        # Ordenação por estado e prioridade
        sorted_missions = sorted(missions_list, key=lambda m: (m.get('state', 'Z'), -m.get('priority', 0)))
        
        for mission in sorted_missions:
            mid = mission.get('mission_id', 'N/A')
            state = mission.get('state', 'N/A').upper()
            rover = mission.get('assigned_rover', 'N/A')
            progress = mission.get('progress_pct', 0.0)
            task = mission.get('task', 'N/A')
            
            # Extrair e formatar Área e Duração
            area_data = mission.get('area')
            duration_s = mission.get('max_duration_s') 
            
            if area_data and isinstance(area_data, dict):
                area_str = (
                    f"Area: ({area_data.get('x1', 0.0):.1f}, {area_data.get('y1', 0.0):.1f}, {area_data.get('z1', 0.0):.1f}) "
                    f"-> ({area_data.get('x2', 0.0):.1f}, {area_data.get('y2', 0.0):.1f}, {area_data.get('z2', 0.0):.1f})"
                )
            else:
                area_str = "Area: N/A"
                
            duration_str = f"Duração Estimada: {duration_s:.1f}s" if duration_s is not None else "Duração: N/A"
            
            # Linha principal da Missão
            line = (
                f"  - ID: {mid} | TAREFA: {task} | ESTADO: {state} | Progresso: {progress:.1f}% | Rover: {rover}"
            )
            # Linha de Detalhes (Área e Duração)
            details_line = (
                f"    -> {area_str} | {duration_str}"
            )
            
            output.append(line)
            output.append(details_line)

        return "\n".join(output)

    def run(self):
        """Loop principal de polling """
        while True:
            logger.info("Polling API...")
            
            # 1. Obter todas as missões (Retorna LISTA)
            missions_raw = self._fetch_api('/api/missions')
            
            # 2. Obter o estado agregado dos rovers 
            rovers_data = self._fetch_api('/api/rovers')

            # --- Processamento e Exibição ---
            
            missions_list = missions_raw if isinstance(missions_raw, list) else []
            rovers_data_dict = rovers_data if isinstance(rovers_data, dict) else {}
            
            print("\n" + "="*50)
            print("         GROUND CONTROL DASHBOARD         ")
            print("="*50)
            print(self._format_charging_stations())
            if missions_list:
                print(self._format_missions(missions_list))
            else:
                print("\n--- MISSÕES ---\n   [Erro ao carregar ou Nenhuma Missão]")
            if rovers_data_dict:
                print(self._format_rovers(rovers_data_dict))
            else:
                print("\n--- ESTADO DOS ROVERS ---\n   [Erro ao carregar ou Nenhum Rover Ativo]")
            
            print("="*50 + "\n")
            
            time.sleep(self.polling_interval)


if __name__ == "__main__":
    import logging
    try:
        from common import config
        # Mantemos o nível WARNING para que apenas mensagens de INFO importantes e o output da dashboard apareçam.
        config.configure_logging(level="WARNING")
    except Exception:
        logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    p = argparse.ArgumentParser(description="Ground Control Client (API Consumer)")
    p.add_argument("--api", dest="api_base", default=f"http://192.168.0.1:{config.API_PORT}", help="Core API base URL")
    p.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds")
    args = p.parse_args()

    client = GroundControlClient(args.api_base, args.interval)
    try:
        client.run()
    except KeyboardInterrupt:
        logger.info("Ground Control Client shutting down.")