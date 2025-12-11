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

Notes on recent changes for the binary TLV protocol:
- The simulator continues to produce a canonical telemetry dict (no JSON encoding here).
- get_telemetry() now returns keys aligned with the canonical fields consumed by the
  binary packers and MissionStore/TelemetryStore (mission_id, progress_pct, position,
  battery_level_pct, status, errors, samples_collected, timestamp_ms).
- Errors are represented as a list (possibly empty) under "errors" to be consistent
  with TLV ERRORS (string) or PAYLOAD_JSON fallback that may embed arrays.
"""
import math
import random
import time
from typing import Dict, Any, Optional, Tuple

class RoverSim:
    # CONSTANTES DE BATERIA (Anteriores)
    LOW_BATTERY_THRESHOLD = 10.0
    EMERGENCY_BATTERY_THRESHOLD = 5.0
    CHARGE_RATE_PER_S = 5.0
    TRAVEL_SPEED = 1.0

    # NOVAS CONSTANTES DE TEMPERATURA
    INITIAL_TEMP = 25.0       # Temperatura inicial/ambiente
    MAX_TEMP = 40.0           # Limite de temperatura para pausa (Overheating)
    COOL_DOWN_TEMP = 30.0     # Limite para retomar a missão
    HEAT_RATE_PER_S = 0.2    # Aumento de 0.1ºC por segundo em RUNNING
    COOL_RATE_PER_S = 1     # Arrefecimento de 1ºC por segundo

    def __init__(self, rover_id: str, position: Tuple[float, float, float] = (0.0, 0.0, 0.0)):
        self.rover_id = rover_id
        self.position = {"x": float(position[0]), "y": float(position[1]), "z": float(position[2])}
        # Adicionado COOLING
        self.state = "IDLE"  # IDLE, RUNNING, COMPLETED, ERROR, CHARGING_TRAVEL, CHARGING, COOLING
        self.current_mission: Optional[Dict[str, Any]] = None
        self.progress_pct: float = 0.0
        self.samples_collected: int = 0
        self.battery_level_pct: float = 100.0
        self.internal_temp_c: float = self.INITIAL_TEMP
        self.current_speed_m_s: float = 0.0 # NOVO CAMPO: Velocidade atual (m/s)
        self.errors: list = []   # list of error dicts, empty when none
        self._elapsed_on_mission: float = 0.0
        self._mission_total_time: float = 10.0  # seconds, default short duration for tests
        self.charging_station_position = {"x": 100.0, "y": 100.0, "z": 0.0} 
        self._elapsed_travel_time: float = 0.0

    def _estimate_mission_total_time(self, mission_spec: Dict[str, Any]) -> float:
        """
        Derive an approximate mission duration (seconds) from mission_spec params.
        Keep conservative and small so tests finish quickly.
        """
        task = mission_spec.get("task", "")
        params = mission_spec.get("params", {}) or {}

        try:
            if task == "capture_images":
                # total time = interval_s * frames (but cap to reasonable max)
                interval = float(params.get("interval_s", 0.1))
                frames = int(params.get("frames", 1))
                total = max(1.0, min(interval * frames, 15.0))
                return total
            elif task == "collect_samples":
                # assume each sample takes some seconds (cap)
                samples = int(params.get("sample_count", 1))
                per_sample = float(params.get("depth_mm", 10.0)) / 100.0 if params.get("depth_mm") else 0.5
                total = max(1.0, min(samples * max(0.5, per_sample), 15.0))
                return total
            elif task == "env_analysis":
                # do a few sampling cycles
                rate = float(params.get("sampling_rate_s", 1.0))
                total = max(1.0, min(rate * 5.0, 15.0))
                return total
        except Exception:
            pass
        # default small duration
        return 6.0

    def start_mission(self, mission_spec: Dict[str, Any]) -> None:
        """
        Begin a mission. mission_spec should include mission_id, task, params, update_interval_s...
        """
        # Adicionar verificação para evitar iniciar missão se a bateria estiver muito baixa
        if self.battery_level_pct < self.LOW_BATTERY_THRESHOLD:
            # Não inicia, e no próximo 'step' irá para CHARGING_TRAVEL
            self.state = "IDLE" 
            return 
            
        self.current_mission = dict(mission_spec)
        self.state = "RUNNING"
        self.progress_pct = 0.0
        self.samples_collected = 0
        self.errors = []
        self._elapsed_on_mission = 0.0
        # compute mission total time from params so tests complete quickly
        self._mission_total_time = float(mission_spec.get("max_duration_s") or self._estimate_mission_total_time(mission_spec))
        # ensure a sensible minimum/maximum
        if self._mission_total_time < 0.5:
            self._mission_total_time = 0.5
        if self._mission_total_time > 60.0:
            self._mission_total_time = 60.0

    def step(self, elapsed_s: float = 1.0) -> None:
        """
        Advance the simulation by elapsed_s seconds.
        Update progress, position (simple random walk), battery, temperature, and handle failures/pauses.
        """
        
        # ----------------------------------------------------
        # Lógica de Arrefecimento (COOLING)
        # ----------------------------------------------------
        if self.state == "COOLING":
            self.current_speed_m_s = 0.0 # Parado
            # Arrefecimento rápido
            self.internal_temp_c = max(self.COOL_DOWN_TEMP, self.internal_temp_c - (self.COOL_RATE_PER_S * elapsed_s))
            if self.internal_temp_c <= self.COOL_DOWN_TEMP:
                # Chegou à temperatura segura (30ºC)
                self.internal_temp_c = self.COOL_DOWN_TEMP
                if self.current_mission and self._elapsed_on_mission < self._mission_total_time:
                    self.state = "RUNNING" # Retoma a missão pausada
                else:
                    self.state = "IDLE" # Missão já estava completa, volta a IDLE
            return # Não avança mais nada neste step
            
        # ----------------------------------------------------
        # Lógica de Carregamento (CHARGING)
        # ----------------------------------------------------
        if self.state == "CHARGING":
            self.current_speed_m_s = 0.0 # Parado
            self.battery_level_pct = min(100.0, self.battery_level_pct + (self.CHARGE_RATE_PER_S * elapsed_s))
            if self.battery_level_pct >= 100.0:
                self.battery_level_pct = 100.0
                self.state = "IDLE"
                self.current_mission = None
                self.errors = [] 
            return

        # ----------------------------------------------------
        # Lógica de Viagem para Carregamento (CHARGING_TRAVEL)
        # ----------------------------------------------------
        if self.state == "CHARGING_TRAVEL":
            self.current_speed_m_s = self.TRAVEL_SPEED # Em movimento
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
            
            # Drenagem da bateria durante a viagem
            drain = elapsed_s * 0.1 
            self.battery_level_pct = max(0.0, self.battery_level_pct - drain)

            if move_distance == distance:
                self.position = dict(target) 
                self.state = "CHARGING"
                self._elapsed_travel_time = 0.0
                self.current_speed_m_s = 0.0 # Acabou de chegar e vai carregar
                return
            
            return 
        
        # ----------------------------------------------------
        # Verificação Pré/Pós-Missão (Bateria < 10% em IDLE ou COMPLETED)
        # ----------------------------------------------------
        if self.state in ("IDLE", "COMPLETED"):
            self.current_speed_m_s = 0.0 # Parado
            # Arrefecimento passivo quando IDLE e acima da temperatura ambiente
            if self.internal_temp_c > self.INITIAL_TEMP:
                self.internal_temp_c = max(self.INITIAL_TEMP, self.internal_temp_c - (self.COOL_RATE_PER_S * 0.1 * elapsed_s))
            
            if self.battery_level_pct < self.LOW_BATTERY_THRESHOLD:
                self.state = "CHARGING_TRAVEL"
                self.current_mission = {"mission_id": f"CHARGE-{time.time()}", "task": "travel_to_charge", "params": {"target_pos": self.charging_station_position}}
                self.progress_pct = 0.0
                self.samples_collected = 0
                self._elapsed_travel_time = 0.0
                if self.state == "COMPLETED":
                     self.errors = [] 
                return 


        # Sai se não estiver em RUNNING
        if self.state != "RUNNING" or not self.current_mission:
            self.current_speed_m_s = 0.0 # Parado ou estado não monitorizado
            return

        # ----------------------------------------------------
        # Lógica de Missão em Execução (RUNNING)
        # ----------------------------------------------------
        self.current_speed_m_s = self.TRAVEL_SPEED * 0.8 # Simulação de movimento em missão a 80% da velocidade máxima
        
        # 1. Aumento de temperatura
        self.internal_temp_c = min(self.MAX_TEMP + self.COOL_RATE_PER_S, self.internal_temp_c + (self.HEAT_RATE_PER_S * elapsed_s))

        # 2. Verificação de Overheating
        if self.internal_temp_c > self.MAX_TEMP:
            self.state = "COOLING" # Pausa a missão
            err = {"code": "TEMP-OVERHEAT", "description": f"Internal temp exceeded {self.MAX_TEMP:.1f}C. Mission paused for cooling."}
            self.errors.append(err)
            self.current_speed_m_s = 0.0 # Parou
            return # Aborta o resto do step de RUNNING (a missão está em pausa)
        
        # 3. Avanço normal da missão (se não estiver em COOLING)
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

        # VERIFICAÇÃO DE EMERGÊNCIA DURANTE A MISSÃO (< 5%)
        if self.battery_level_pct < self.EMERGENCY_BATTERY_THRESHOLD:
             err = {"code": "BAT-EMERGENCY-ABORT", "description": f"Battery critically low ({self.battery_level_pct:.1f}%). Mission aborted. Returning to charge."}
             self.errors.append(err)
             self.state = "CHARGING_TRAVEL" 
             self.progress_pct = 0.0
             self._elapsed_on_mission = 0.0
             self.current_mission = {"mission_id": f"EMERG-CHARGE-{time.time()}", "task": "travel_to_charge", "params": {"target_pos": self.charging_station_position}}
             self._elapsed_travel_time = 0.0
             self.current_speed_m_s = self.TRAVEL_SPEED # Começa a viajar para carregar
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
            # env_analysis: use samples_collected as number of sample cycles completed
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
            self.current_speed_m_s = 0.0
            # Ensure samples_collected final values
            if task == "collect_samples":
                self.samples_collected = int(params.get("sample_count", 1))
            elif task == "capture_images":
                self.samples_collected = int(params.get("frames", 1))
            else:
                self.samples_collected = max(1, int(self.samples_collected))

    def is_mission_complete(self) -> bool:
        # CORREÇÃO: Apenas o estado COMPLETED sinaliza o término da missão principal
        return self.state == "COMPLETED"

    def get_telemetry(self) -> Dict[str, Any]:
        """
        Return a telemetry dict used by ml_client to build PROGRESS/MISSION_COMPLETE bodies.

        The dict matches the canonical fields expected by the binary TLV packers / MissionStore:
          - mission_id, progress_pct, position, battery_level_pct, status, errors, samples_collected, timestamp_ms).
        
        NOTE: progress_pct é incluído para permitir o envio do TLV de progresso.
        """
        ts_ms = int(time.time() * 1000)

        # Mapeia os estados
        status_report = self.state.lower()
        if status_report == "charging_travel":
            status_report = "traveling_to_charge"
        elif status_report == "charging":
            status_report = "charging"
        elif status_report == "cooling":
            status_report = "cooling_down" # Mapeia para um nome mais descritivo na telemetria
        
        return {
            "mission_id": (self.current_mission.get("mission_id") if self.current_mission else None),
            "progress_pct": self.progress_pct, 
            "position": dict(self.position),
            "battery_level_pct": round(self.battery_level_pct, 1),
            "internal_temp_c": round(self.internal_temp_c, 1), # NOVO CAMPO
            "current_speed_m_s": round(self.current_speed_m_s, 1), # NOVO CAMPO: Velocidade
            "status": status_report,
            "errors": list(self.errors),
            "samples_collected": int(self.samples_collected),
            "timestamp_ms": ts_ms,
        }