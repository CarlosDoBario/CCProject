#!/usr/bin/env python3
"""
gc_cliente.py

Ground Control Client: Consome a API de Observação da Nave-Mãe (via HTTP REST)
e exibe o estado das missões e a telemetria dos rovers.
"""
import argparse
import time
import json
import logging
from typing import Dict, Any, List, Optional
import requests # Assumimos que requests está instalado (pip install requests)

from common import config, utils

logger = utils.get_logger("groundcontrol.client")


class GroundControlClient:
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
        
    def _format_telemetry(self, latest_telemetry: Dict[str, Dict[str, Any]]) -> str:
        """Formata a telemetria mais recente para exibição."""
        if not latest_telemetry:
            return "   [Nenhuma Telemetria Recebida]\n"
        
        output = ["\n--- ÚLTIMA TELEMETRIA (TS/ML) ---"]
        for rid, data in latest_telemetry.items():
            # Extrair campos customizados e garantir que são float/dict
            temp = data.get("internal_temp_c", 0.0)
            speed = data.get("current_speed_m_s", 0.0)
            pos = data.get("position", {"x": 0.0, "y": 0.0})
            batt = data.get("battery_level_pct", 0.0)
            
            line = (
                f"  > Rover {rid}: Estado: {data.get('status', 'N/A').upper()} | "
                f"Bateria: {batt:.1f}% | "
                f"Posição: ({pos['x']:.1f}, {pos['y']:.1f}) | "
                f"Temp: {temp:.1f} °C | "
                f"Velocidade: {speed:.1f} m/s"
            )
            output.append(line)
        return "\n".join(output)
        
    def _format_rovers(self, rovers_status: Dict[str, Dict[str, Any]]) -> str:
        """Formata o estado dos rovers (com base no TelemetryStore)."""
        if not rovers_status:
            return "   [Nenhum Rover Ativo]\n"
            
        output = ["\n--- ESTADO DOS ROVERS ---"]
        for rid, status in rovers_status.items():
            # Os campos detalhados vêm diretamente do payload do API Server agora
            batt = status.get('battery_level_pct', 0.0)
            temp = status.get('internal_temp_c', 0.0)
            
            line = (
                f"  - Rover ID: {rid} | ESTADO: {status.get('state', 'DESCONHECIDO').upper()} | "
                f"Bateria: {batt:.1f}% | "
                f"Temperatura: {temp:.1f}°C"
            )
            output.append(line)
        return "\n".join(output)

    def _format_missions(self, missions_list: List[Dict[str, Any]]) -> str:
        """Formata a lista de missões (recebendo uma LISTA da API)."""
        if not missions_list:
            return "   [Nenhuma Missão Criada]\n"
            
        output = ["\n--- MISSÕES ---"]
        
        # Iterar diretamente sobre a lista retornada pela API e ordenar
        sorted_missions = sorted(missions_list, key=lambda m: (m.get('state', 'Z'), -m.get('priority', 0)))
        
        for mission in sorted_missions:
            mid = mission.get('mission_id', 'N/A')
            state = mission.get('state', 'N/A').upper()
            rover = mission.get('assigned_rover', 'N/A')
            # O campo é progress_pct (corrigido na API)
            progress = mission.get('progress_pct', 0.0)
            task = mission.get('task', 'N/A')
            
            line = (
                f"  - ID: {mid} | TAREFA: {task} | ESTADO: {state} | "
                f"Rover: {rover} | Progresso: {progress:.1f}%"
            )
            output.append(line)
        return "\n".join(output)

    def run(self):
        """Loop principal de polling."""
        while True:
            logger.info("Polling API...")
            
            # 1. Obter todas as missões (Retorna LISTA)
            missions_raw = self._fetch_api('/api/missions')
            
            # 2. Obter o estado agregado dos rovers (Retorna DICIONÁRIO)
            rovers_data = self._fetch_api('/api/rovers')

            # 3. Obter a telemetria mais recente (Retorna DICIONÁRIO: {"latest_telemetry": {...}})
            telemetry_data = self._fetch_api('/api/telemetry/latest')

            # --- Processamento e Exibição ---
            
            # CORREÇÃO 1: Tratar a resposta de /api/missions como uma LISTA
            missions_list = missions_raw if isinstance(missions_raw, list) else []
            
            # CORREÇÃO 2: Tratar a resposta de /api/rovers como um DICIONÁRIO
            rovers_data_dict = rovers_data if isinstance(rovers_data, dict) else {}

            # CORREÇÃO 3: Extrair a telemetria detalhada
            latest_telemetry = telemetry_data.get('latest_telemetry', {}) if isinstance(telemetry_data, dict) else {}
            
            print("\n" + "="*50)
            print("         GROUND CONTROL DASHBOARD         ")
            print("="*50)

            # Exibir Missões
            if missions_list:
                print(self._format_missions(missions_list))
            else:
                print("\n--- MISSÕES ---\n   [Erro ao carregar ou Nenhuma Missão]")

            # Exibir Estado dos Rovers
            if rovers_data_dict:
                print(self._format_rovers(rovers_data_dict))
            else:
                print("\n--- ESTADO DOS ROVERS ---\n   [Erro ao carregar ou Nenhum Rover Ativo]")
            
            # Exibir Telemetria Detalhada
            if latest_telemetry:
                print(self._format_telemetry(latest_telemetry))
            else:
                print("\n--- ÚLTIMA TELEMETRIA (TS/ML) ---\n   [Erro ao carregar ou Nenhuma Telemetria]")

            print("="*50 + "\n")
            
            time.sleep(self.polling_interval)


if __name__ == "__main__":
    # Configuração explícita de logging para garantir output imediato
    import logging
    try:
        config.configure_logging(level="WARNING")
    except Exception:
        logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    p = argparse.ArgumentParser(description="Ground Control Client (API Consumer)")
    p.add_argument("--api", dest="api_base", default=f"http://127.0.0.1:{config.API_PORT}", help="Core API base URL")
    p.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds")
    args = p.parse_args()

    client = GroundControlClient(args.api_base, args.interval)
    try:
        client.run()
    except KeyboardInterrupt:
        logger.info("Ground Control Client shutting down.")