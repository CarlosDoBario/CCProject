#!/usr/bin/env python3
"""
rover_sim.py

RoverSim minimal simulator used by ml_client in tests.

Provides:
- class RoverSim(rover_id, position=(x,y,z))
- start_mission(mission_spec)
- step(elapsed_s)
- get_telemetry()
- is_mission_complete()
"""
import math
import random
import time
from typing import Dict, Any, Optional, Tuple

class RoverSim:
    # CONSTANTES DE BATERIA
    LOW_BATTERY_THRESHOLD_IDLE = 25.0       # 25% para ir carregar (IDLE, MOVING, etc.)
    LOW_BATTERY_THRESHOLD_RUNNING = 20.0    # 20% para pausar missão e ir carregar (IN_MISSION/RUNNING)
    CHARGE_RATE_PER_S = 5.0
    TRAVEL_SPEED = 1.0

    # CONSTANTES DE TEMPERATURA
    INITIAL_TEMP = 25.0       # 25.0°C (Temperatura inicial e ponto final de arrefecimento)
    MAX_TEMP = 35.0           # 35.0°C (Limite de temperatura para pausa)
    HEAT_RATE_PER_S = 0.2
    COOL_RATE_PER_S = 1

    # CONSTANTES DE MOVIMENTO
    MOVE_THRESHOLD = 5.0 

    # POSIÇÕES DOS POSTOS DE CARREGAMENTO (CONFIGURAÇÃO DE TESTE)
    CHARGING_STATION_A = {"x": 45.0, "y": 45.0, "z": 0.0}
    CHARGING_STATION_B = {"x": 24.0, "y": 24.0, "z": 0.0}

    def __init__(self, rover_id: str, position: Tuple[float, float, float] = (65.0, 65.0, 0.0)): # Posição inicial definida
        self.rover_id = rover_id
        self.position = {"x": float(position[0]), "y": float(position[1]), "z": float(position[2])}
        self.state = "IDLE"  # IDLE, RUNNING, COMPLETED, ERROR, CHARGING_TRAVEL, CHARGING, COOLING, MOVING_TO_MISSION
        self.current_mission: Optional[Dict[str, Any]] = None
        self._paused_mission_spec: Optional[Dict[str, Any]] = None # NOVO: Para guardar a missão interrompida
        self.progress_pct: float = 0.0
        self.samples_collected: int = 0
        self.battery_level_pct: float = 30.0 # Bateria inicial definida a 30%
        self.internal_temp_c: float = self.INITIAL_TEMP
        self.current_speed_m_s: float = 0.0
        self.errors: list = []
        self.current_target = {"x": 0.0, "y": 0.0, "z": 0.0}
        self._elapsed_on_mission: float = 0.0
        self._mission_total_time: float = 10.0
        self.charging_station_position = self.CHARGING_STATION_A
        self._elapsed_travel_time: float = 0.0

    def _estimate_mission_total_time(self, mission_spec: Dict[str, Any]) -> float:
        """Derive an approximate mission duration (seconds)."""
        task = mission_spec.get("task", "")
        params = mission_spec.get("params", {}) or {}

        try:
            if task == "capture_images":
                interval = float(params.get("interval_s", 0.1))
                frames = int(params.get("frames", 1))
                total = max(1.0, min(interval * frames, 15.0))
                return total
            elif task == "collect_samples":
                samples = int(params.get("sample_count", 1))
                per_sample = float(params.get("depth_mm", 10.0)) / 100.0 if params.get("depth_mm") else 0.5
                total = max(1.0, min(samples * max(0.5, per_sample), 15.0))
                return total
            elif task == "env_analysis":
                rate = float(params.get("sampling_rate_s", 1.0))
                total = max(1.0, min(rate * 5.0, 15.0))
                return total
        except Exception:
            pass
        return 6.0

    def _calculate_distance(self, p1, p2) -> float:
        """Calcula a distância 3D entre dois pontos."""
        return math.sqrt((p1["x"] - p2["x"])**2 + (p1["y"] - p2["y"])**2 + (p1.get("z", 0.0) - p2.get("z", 0.0))**2)

    def _find_nearest_station(self, current_pos: Dict[str, float]) -> Dict[str, float]:
        """Encontra a estação de carregamento mais próxima do rover."""
        stations = [self.CHARGING_STATION_A, self.CHARGING_STATION_B]
        
        nearest = None
        min_distance = float('inf')
        
        for station in stations:
            distance = self._calculate_distance(current_pos, station)
            if distance < min_distance:
                min_distance = distance
                nearest = station
                
        return nearest if nearest else self.CHARGING_STATION_A

    def _do_redirect_to_charge(self, threshold: float, is_running_mission: bool):
        """Executa a transição para CHARGING_TRAVEL, encontra o posto mais próximo e define a missão de carga."""
        
        err_code = "BAT-PAUSE-CHARGE"
        description = f"Battery critically low ({self.battery_level_pct:.1f}%). Redirecting to charge."
        if is_running_mission:
            description = f"Battery critically low ({self.battery_level_pct:.1f}%). Mission paused. Returning to charge."
        
        self.errors.append({"code": err_code, "description": description})
        self.state = "CHARGING_TRAVEL" 
        
        # 1. CRITICAL FIX: GUARDAR O ESTADO COMPLETO DA MISSÃO ATUAL (current_mission)
        if self.current_mission and self.current_mission.get("task") not in ("travel_to_charge", "cooling"):
             # Salva o progresso e a posição para a retomada
            paused_spec = dict(self.current_mission)
            paused_spec['progress_pct'] = self.progress_pct
            paused_spec['elapsed_time'] = self._elapsed_on_mission
            paused_spec['current_pos'] = dict(self.position) # Salva a posição de interrupção
            self._paused_mission_spec = paused_spec
        
        # 2. Configurar a missão de emergência
        nearest_station = self._find_nearest_station(self.position)
        self.charging_station_position = nearest_station
        
        self.current_mission = {"mission_id": f"EMERG-CHARGE-{time.time()}", "task": "travel_to_charge", "params": {"target_pos": self.charging_station_position}}
        self._elapsed_travel_time = 0.0
        self.current_speed_m_s = self.TRAVEL_SPEED
    

    def start_mission(self, mission_spec: Dict[str, Any]) -> None:
        """
        Inicia uma missão, verificando primeiro se é necessário viajar até à área de missão.
        
        Pode ser chamada para:
        1. Iniciar uma nova missão.
        2. Retomar uma missão pausada.
        """
        # Se a bateria estiver demasiado baixa, inicia imediatamente o processo de carregamento
        if self.battery_level_pct < self.LOW_BATTERY_THRESHOLD_IDLE:
            # Não pode aceitar a nova missão. Redireciona para o posto de carga.
            self._do_redirect_to_charge(self.LOW_BATTERY_THRESHOLD_IDLE, is_running_mission=False) 
            return 
            
        self.current_mission = dict(mission_spec)
        self.errors = []
        
        # 1. TENTATIVA DE RETOMADA: Verifica se há dados de progresso na nova missão (usado para retomar)
        is_resumption = 'elapsed_time' in mission_spec
        
        if is_resumption:
            self._elapsed_on_mission = float(mission_spec.get('elapsed_time', 0.0))
            self.progress_pct = float(mission_spec.get('progress_pct', 0.0))
            
            # Se a missão pausada guardou a posição de interrupção, usa-a como ponto de partida
            if 'current_pos' in mission_spec:
                 self.position = dict(mission_spec['current_pos'])
            
            # O target é o mesmo da missão original (centro da área)
            area = mission_spec.get("area")
            if area:
                target_x = (area.get("x1", 0.0) + area.get("x2", 0.0)) / 2.0
                target_y = (area.get("y1", 0.0) + area.get("y2", 0.0)) / 2.0
                target_z = (area.get("z1", 0.0) + area.get("z2", 0.0)) / 2.0
            else:
                target_x, target_y, target_z = 0.0, 0.0, 0.0
            
            self.current_target = {"x": target_x, "y": target_y, "z": target_z}
            
        else:
            # 2. Determinar o Ponto de Partida da Missão (Centro da Área)
            area = mission_spec.get("area")
            if area:
                target_x = (area.get("x1", 0.0) + area.get("x2", 0.0)) / 2.0
                target_y = (area.get("y1", 0.0) + area.get("y2", 0.0)) / 2.0
                target_z = (area.get("z1", 0.0) + area.get("z2", 0.0)) / 2.0
            else:
                target_x, target_y, target_z = 0.0, 0.0, 0.0

            self.current_target = {"x": target_x, "y": target_y, "z": target_z}
            self.progress_pct = 0.0
            self.samples_collected = 0
            self._elapsed_on_mission = 0.0
            
            
        # 3. Definir o tempo total da missão (necessário para calcular o progresso)
        self._mission_total_time = float(mission_spec.get("max_duration_s") or self._estimate_mission_total_time(mission_spec))
        if self._mission_total_time < 0.5:
            self._mission_total_time = 0.5
        if self._mission_total_time > 60.0:
            self._mission_total_time = 60.0
            
        # 4. Verificar Distância e Definir Estado Inicial (Movimento vs. Execução)
        dx = self.position["x"] - self.current_target["x"]
        dy = self.position["y"] - self.current_target["y"]
        distance = math.sqrt(dx**2 + dy**2)
        
        # MODIFICADO: A condição 'and not is_resumption' foi removida.
        if distance > self.MOVE_THRESHOLD:
            # Se estiver longe (após ir carregar, por exemplo), move-se para o alvo da missão.
            self.state = "MOVING_TO_MISSION"
        elif self.progress_pct < 100.0:
            # Se for retomada ou nova missão já perto, e não estiver completa, corre.
            self.state = "RUNNING"
        else:
             # Caso a retomada chegue já a 100% (erro de estado), marca como completa.
            self.state = "COMPLETED"
        

    def step(self, elapsed_s: float = 1.0) -> None:
        """Advance the simulation by elapsed_s seconds."""
        
        # ----------------------------------------------------
        # Lógica de Viagem para Missão (MOVING_TO_MISSION)
        # ----------------------------------------------------
        if self.state == "MOVING_TO_MISSION":
            self.current_speed_m_s = self.TRAVEL_SPEED
            target = self.current_target
            current = self.position
            
            # 1. Drenagem da bateria (realizada primeiro)
            drain = elapsed_s * 0.2
            self.battery_level_pct = max(0.0, self.battery_level_pct - drain)

            # 2. VERIFICAÇÃO CRÍTICA DE BATERIA (25% LIMITE IDLE/MOVING)
            # USAMOS O <= PARA GARANTIR QUE 25.0% É INCLUÍDO
            if self.battery_level_pct <= self.LOW_BATTERY_THRESHOLD_IDLE:
                self._do_redirect_to_charge(self.LOW_BATTERY_THRESHOLD_IDLE, is_running_mission=False)
                return # Redirecionado para CHARGING_TRAVEL

            # 3. Continuação da lógica de movimento
            dx = target["x"] - current["x"]
            dy = target["y"] - current["y"]
            distance = math.sqrt(dx**2 + dy**2)
            
            move_distance = min(self.TRAVEL_SPEED * elapsed_s, distance) 
            
            if distance > 0:
                ratio = move_distance / distance
                self.position["x"] += dx * ratio
                self.position["y"] += dy * ratio

            # Aumento de temperatura em MOVING_TO_MISSION
            self.internal_temp_c = min(self.MAX_TEMP + self.COOL_RATE_PER_S, self.internal_temp_c + (self.HEAT_RATE_PER_S * elapsed_s))

            # Verificação de Overheating (35°C)
            if self.internal_temp_c > self.MAX_TEMP:
                # CRITICAL FIX 4: Salvar a missão pausada também aqui (se necessário)
                if self.current_mission and self.current_mission.get("task") not in ("travel_to_charge", "cooling"):
                    paused_spec = dict(self.current_mission)
                    paused_spec['progress_pct'] = self.progress_pct
                    paused_spec['elapsed_time'] = self._elapsed_on_mission
                    paused_spec['current_pos'] = dict(self.position) 
                    self._paused_mission_spec = paused_spec
                
                self.state = "COOLING" 
                self.current_mission = {"mission_id": f"EMERG-COOL-{time.time()}", "task": "cooling"}
                err = {"code": "TEMP-OVERHEAT", "description": f"Internal temp exceeded {self.MAX_TEMP:.1f}C. Mission paused for cooling."}
                self.errors.append(err)
                self.current_speed_m_s = 0.0
                return # Aborta o resto do step

            if move_distance == distance or distance < self.MOVE_THRESHOLD:
                self.position["x"] = target["x"]
                self.position["y"] = target["y"]
                self.state = "RUNNING"
                self.current_speed_m_s = self.TRAVEL_SPEED * 0.8 
                return
            
            return
        
        # ----------------------------------------------------
        # Lógica de Arrefecimento (COOLING)
        # ----------------------------------------------------
        if self.state == "COOLING":
            self.current_speed_m_s = 0.0
            # ARREFECIMENTO: até atingir 25.0°C (INITIAL_TEMP)
            self.internal_temp_c = max(self.INITIAL_TEMP, self.internal_temp_c - (self.COOL_RATE_PER_S * elapsed_s))
            if self.internal_temp_c <= self.INITIAL_TEMP: 
                self.internal_temp_c = self.INITIAL_TEMP
                
                # FIX: Ao arrefecer, volta para IDLE, confiando no Agente para pedir a retoma.
                self.state = "IDLE" 
                self.current_mission = None
                self.errors = [] # Limpa os erros de temperatura
            return
            
        # ----------------------------------------------------
        # Lógica de Carregamento (CHARGING)
        # ----------------------------------------------------
        if self.state == "CHARGING":
            self.current_speed_m_s = 0.0
            self.battery_level_pct = min(100.0, self.battery_level_pct + (self.CHARGE_RATE_PER_S * elapsed_s))
            if self.battery_level_pct >= 100.0:
                self.battery_level_pct = 100.0
                
                # FIX: Ao carregar, volta para IDLE, confiando no Agente para pedir a retoma.
                # A missão original pausada (M-0003) fica guardada em self._paused_mission_spec.
                self.state = "IDLE"
                
                # A missão corrente (EMERG-CHARGE) é limpa. 
                self.current_mission = None
                self.errors = [] 
            return

        # ----------------------------------------------------
        # Lógica de Viagem para Carregamento (CHARGING_TRAVEL)
        # ----------------------------------------------------
        if self.state == "CHARGING_TRAVEL":
            self.current_speed_m_s = self.TRAVEL_SPEED
            self._elapsed_travel_time += elapsed_s
            
            target = self.charging_station_position 
            current = self.position
            
            dx = target["x"] - current["x"]
            dy = target["y"] - current["y"]
            distance = math.sqrt(dx**2 + dy**2)
            
            move_distance = min(self.TRAVEL_SPEED * elapsed_s, distance) 
            
            if distance > 0:
                ratio = move_distance / distance
                self.position["x"] += dx * ratio
                self.position["y"] += dy * ratio
            
            drain = elapsed_s * 0.1 
            self.battery_level_pct = max(0.0, self.battery_level_pct - drain)

            if move_distance == distance:
                self.position = dict(target) 
                self.state = "CHARGING" # ESTADO DE CARREGAMENTO NO POSTO
                self._elapsed_travel_time = 0.0
                self.current_speed_m_s = 0.0
                return
            
            return 
        
        # ----------------------------------------------------
        # Verificação Pré/Pós-Missão (IDLE/COMPLETED)
        # ----------------------------------------------------
        if self.state in ("IDLE", "COMPLETED"):
            self.current_speed_m_s = 0.0 # GARANTE que está parado
            
            # Arrefecimento passivo
            if self.internal_temp_c > self.INITIAL_TEMP:
                self.internal_temp_c = max(self.INITIAL_TEMP, self.internal_temp_c - (self.COOL_RATE_PER_S * 0.1 * elapsed_s))
            
            # Verificar necessidade de carga (Limite: 25%)
            if self.battery_level_pct <= self.LOW_BATTERY_THRESHOLD_IDLE:
                self._do_redirect_to_charge(self.LOW_BATTERY_THRESHOLD_IDLE, is_running_mission=False)
                return 
            
            # Se não precisou de carregar, para de processar (IDLE)
            return 


        # Sai se não estiver em RUNNING (Este bloco só é atingido se o estado for algo inesperado)
        if self.state != "RUNNING" or not self.current_mission:
            self.current_speed_m_s = 0.0
            return

        # ----------------------------------------------------
        # Lógica de Missão em Execução (RUNNING)
        # ----------------------------------------------------
        if self.state == "RUNNING" and self.current_mission:
            self.current_speed_m_s = self.TRAVEL_SPEED * 0.8 
            
            # 1. Aumento de temperatura
            self.internal_temp_c = min(self.MAX_TEMP + self.COOL_RATE_PER_S, self.internal_temp_c + (self.HEAT_RATE_PER_S * elapsed_s))

            # 2. Verificação de Overheating (35°C)
            if self.internal_temp_c > self.MAX_TEMP:
                 # CRITICAL FIX 5: Salvar o estado antes de pausar por temperatura
                if self.current_mission and self.current_mission.get("task") not in ("travel_to_charge", "cooling"):
                    paused_spec = dict(self.current_mission)
                    paused_spec['progress_pct'] = self.progress_pct
                    paused_spec['elapsed_time'] = self._elapsed_on_mission
                    paused_spec['current_pos'] = dict(self.position) 
                    self._paused_mission_spec = paused_spec
                
                self.state = "COOLING" 
                self.current_mission = {"mission_id": f"EMERG-COOL-{time.time()}", "task": "cooling"}
                err = {"code": "TEMP-OVERHEAT", "description": f"Internal temp exceeded {self.MAX_TEMP:.1f}C. Mission paused for cooling."}
                self.errors.append(err)
                self.current_speed_m_s = 0.0
                return
            
            # 3. Avanço normal da missão
            self._elapsed_on_mission += float(elapsed_s)

            # Update progress proportionally to elapsed time vs total_time
            fraction = min(1.0, self._elapsed_on_mission / self._mission_total_time)
            self.progress_pct = round(fraction * 100.0, 2)

            # simple position update (small move)
            dx = random.uniform(-0.5, 0.5) * (elapsed_s / max(1.0, self._mission_total_time))
            dy = random.uniform(-0.5, 0.5) * (elapsed_s / max(1.0, self._mission_total_time))
            self.position["x"] += dx
            self.position["y"] += dy

            # battery drain proportional to elapsed time (small)
            drain = (elapsed_s / max(1.0, self._mission_total_time)) * 2.0
            self.battery_level_pct = max(0.0, self.battery_level_pct - drain)

            # VERIFICAÇÃO DE EMERGÊNCIA DURANTE A MISSÃO (< 20%)
            if self.battery_level_pct <= self.LOW_BATTERY_THRESHOLD_RUNNING:
                 self._do_redirect_to_charge(self.LOW_BATTERY_THRESHOLD_RUNNING, is_running_mission=True)
                 return

            # samples collected heuristic
            task = self.current_mission.get("task", "")
            params = self.current_mission.get("params", {}) or {}
            if task == "collect_samples":
                target = int(params.get("sample_count", 1))
                self.samples_collected = min(target, int(self.progress_pct / 100.0 * target))
            elif task == "capture_images":
                frames = int(params.get("frames", 1))
                self.samples_collected = min(frames, int(self.progress_pct / 100.0 * frames))
            else:
                self.samples_collected = int(self.progress_pct / 100.0 * 5)

            # Small probability to inject a transient non-critical error (rare)
            if random.random() < 0.0005:
                err = {"code": "SIM-RAND-ERR", "description": "Transient simulated sensor glitch"}
                self.errors.append(err)
                self.state = "ERROR"
                self.current_speed_m_s = 0.0
                return

            # Completion check
            if self._elapsed_on_mission >= self._mission_total_time or self.progress_pct >= 100.0:
                self.progress_pct = 100.0
                self.state = "COMPLETED"
                self.current_mission = None # Missão concluída é limpa
                self.current_speed_m_s = 0.0
                if task == "collect_samples":
                    self.samples_collected = int(params.get("sample_count", 1))
                elif task == "capture_images":
                    self.samples_collected = int(params.get("frames", 1))
                else:
                    self.samples_collected = max(1, int(self.samples_collected))

    def is_mission_complete(self) -> bool:
        # APENAS COMPLETED deve reportar conclusão final ao servidor ML.
        return self.state == "COMPLETED"

    def get_telemetry(self) -> Dict[str, Any]:
        """Return a telemetry dict used by ml_client to build PROGRESS/MISSION_COMPLETE bodies."""
        ts_ms = int(time.time() * 1000)

        # Mapeia os estados
        status_report = self.state.lower()
        if status_report == "charging_travel":
            status_report = "traveling_to_charge"
        elif status_report == "charging":
            status_report = "charging" 
        elif status_report == "cooling":
            status_report = "cooling_down" 
        elif status_report == "moving_to_mission":
            status_report = "moving" 
        
        return {
            "mission_id": (self.current_mission.get("mission_id") if self.current_mission else None),
            "progress_pct": round(self.progress_pct, 1), 
            "position": dict(self.position),
            "battery_level_pct": round(self.battery_level_pct, 1),
            "internal_temp_c": round(self.internal_temp_c, 1), 
            "current_speed_m_s": round(self.current_speed_m_s, 1),
            "status": status_report,
            "errors": list(self.errors),
            "samples_collected": int(self.samples_collected),
            "timestamp_ms": ts_ms,
        }