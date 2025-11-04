#!/usr/bin/env python3
"""
rover_sim.py
Simulador de alto-nível do Rover — modela posição 3D (x,y,z), bateria, consumo por ação,
e injecção de falhas aplicacionais (locomoção, amostragem, sensores).

Objetivo
- Fornecer uma componente de simulação que pode ser integrada ao ml_client.py para:
  - calcular posições/waypoints e progresso da missão,
  - atualizar nível de bateria conforme ações,
  - decidir quando reportar erros simulados (falhas) e qual o seu efeito,
  - fornecer métricas locais (samples_collected, etc.)
- Pode ser usado standalone para observar o comportamento do rover sem ligar ao servidor.

Design e integração
- Classe principal: RoverSim
  - Métodos principais:
    - start_mission(mission_spec): inicializa dados internos (target area, task, params)
    - step(dt_s): avança a simulação dt_s segundos (movimenta, consome bateria, executa amostragens/imagens)
    - get_telemetry(): retorna um dicionário compatível com o campo "body" usado nas mensagens PROGRESS/TELEMETRY
    - inject_failure(kind): força um evento de falha (locomotion, sensor, system, battery)
    - is_mission_complete(): indica se missão terminou
  - Propriedades: position (x,y,z), battery_level_pct, state, samples_collected, last_error
- Falhas:
  - Probabilísticas: p_move_fail, p_sample_fail, p_system_fail configuráveis.
  - Ao ocorrer uma falha, RoverSim regista last_error e coloca state em "ERROR" (o cliente decide como reagir: reportar e tentar recuperar).
- Interface com ml_client:
  - O cliente pode criar uma instância RoverSim(rover_id, initial_pos=(...))
  - Ao receber MISSION_ASSIGN, o ml_client passa o body para start_mission(...)
  - Na rotina de progressos, em vez de valores dummy, o cliente chama step(update_interval) e usa get_telemetry() para preencher o body do PROGRESS.

Usos
- Execução standalone:
  - PYTHONPATH=src python3 src/rover/rover_sim.py --demo
  - Mostra uma simulação textual da execução de uma missão demo.

Notas
- O simulador é intencionalmente simples e determinístico quando as probabilidades de falha são zero.
- Pode ser ampliado para trajetórias complexas (waypoints), curvas de consumo energia baseadas em terreno, armazenamento de logs, etc.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple, List
import math
import random
import time
import threading

from common import utils

logger = utils.get_logger("ml.rover_sim")


@dataclass
class RoverSim:
    rover_id: str
    position: Tuple[float, float, float] = (0.0, 0.0, 0.0)  # x,y,z in meters
    battery_level_pct: float = 100.0
    state: str = "IDLE"  # IDLE, MOVING, SAMPLING, CAPTURING, ANALYZING, CHARGING, ERROR, COMPLETED
    samples_collected: int = 0
    payload_capacity_pct: float = 100.0
    last_error: Optional[Dict[str, Any]] = None

    # mission runtime fields (set by start_mission)
    current_mission: Optional[Dict[str, Any]] = None
    mission_start_ts: Optional[float] = None
    mission_progress_pct: float = 0.0

    # failure probabilities (per-step or per-action)
    p_move_fail: float = 0.01
    p_sample_fail: float = 0.02
    p_system_fail: float = 0.005

    # motion model
    speed_m_s: float = 0.5  # default speed when moving
    target_point: Optional[Tuple[float, float, float]] = None

    # internal
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def start_mission(self, mission_spec: Dict[str, Any]) -> None:
        """
        Inicializa a missão no simulador a partir do mission_spec (o body do MISSION_ASSIGN).
        Espera mission_spec contendo keys: mission_id, area, task, params, update_interval_s.
        """
        with self._lock:
            self.current_mission = mission_spec.copy()
            self.mission_start_ts = time.time()
            self.mission_progress_pct = 0.0
            self.samples_collected = 0
            self.state = "ASSIGNED"
            # Decide um ponto alvo simples: centro da área (usa z se presente)
            area = mission_spec.get("area")
            if area:
                x1 = area.get("x1", 0.0)
                y1 = area.get("y1", 0.0)
                z1 = area.get("z1", 0.0)
                x2 = area.get("x2", x1)
                y2 = area.get("y2", y1)
                z2 = area.get("z2", z1)
                cx = (x1 + x2) / 2.0
                cy = (y1 + y2) / 2.0
                cz = (z1 + z2) / 2.0
                self.target_point = (cx, cy, cz)
            else:
                # if no area given, target is offset from current position
                self.target_point = (self.position[0] + 5.0, self.position[1] + 0.0, self.position[2])
            # Task-specific initialization
            task = mission_spec.get("task", "capture_images")
            params = mission_spec.get("params", {})
            if task == "collect_samples":
                # expect depth_mm, sample_count
                self.current_mission.setdefault("params", {}).setdefault("sample_count", params.get("sample_count", 1))
            if task == "capture_images":
                self.current_mission.setdefault("params", {}).setdefault("frames", params.get("frames", 10))
            if task == "env_analysis":
                self.current_mission.setdefault("params", {}).setdefault("sampling_rate_s", params.get("sampling_rate_s", 10))
            # set state to MOVING to approach target
            self.state = "MOVING"
            logger.info(f"{self.rover_id}: mission {self.current_mission.get('mission_id')} started, target={self.target_point}")

    def step(self, dt_s: float) -> None:
        """
        Avança a simulação dt_s segundos:
        - se MOVING: move em direção a target_point a self.speed_m_s
        - ao chegar ao alvo: executa a ação conforme task (sampling/imaging/analysis) progressivamente
        - consome bateria por segundo conforme atividade
        - injeta falhas probabilísticas
        """
        with self._lock:
            if self.state in ("IDLE", "CHARGING", "ERROR", "COMPLETED"):
                # minimal idle or not executing
                self._idle_drain(dt_s)
                return

            # Random system failure check
            if random.random() < 1 - math.pow((1 - self.p_system_fail), dt_s):
                self._set_error("E-SYS-01", "system_error", "Random system failure")
                return

            if self.state == "MOVING":
                self._move_towards_target(dt_s)
            elif self.state == "SAMPLING":
                self._perform_sampling(dt_s)
            elif self.state == "CAPTURING":
                self._perform_capture(dt_s)
            elif self.state == "ANALYZING":
                self._perform_analysis(dt_s)
            # battery checks
            if self.battery_level_pct <= 5.0:
                self._set_error("E-BATT-LOW", "battery", "Battery critically low")
                return

    def _idle_drain(self, dt_s: float) -> None:
        drain = 0.001 * dt_s  # 0.001% per second idle
        self.battery_level_pct = max(0.0, self.battery_level_pct - drain)

    def _move_towards_target(self, dt_s: float) -> None:
        if not self.target_point:
            self.state = "IDLE"
            return
        tx, ty, tz = self.target_point
        x, y, z = self.position
        dx = tx - x
        dy = ty - y
        dz = tz - z
        dist = math.sqrt(dx * dx + dy * dy + dz * dz)
        if dist < 0.1:
            # reached
            self.position = (tx, ty, tz)
            # transition to task-specific action
            task = self.current_mission.get("task", "capture_images")
            if task == "collect_samples":
                self.state = "SAMPLING"
            elif task == "capture_images":
                self.state = "CAPTURING"
            else:
                self.state = "ANALYZING"
            logger.info(f"{self.rover_id}: reached target, switching to {self.state}")
            return

        # possible locomotion failure during movement
        if random.random() < 1 - math.pow((1 - self.p_move_fail), dt_s):
            self._set_error("E-LOC-01", "locomotion", "Locomotion failure while moving")
            return

        # move proportional to dt_s
        travel = min(self.speed_m_s * dt_s, dist)
        nx = x + (dx / dist) * travel
        ny = y + (dy / dist) * travel
        nz = z + (dz / dist) * travel
        self.position = (nx, ny, nz)
        # battery drain per meter
        energy_cost = 0.02 * travel  # 0.02% per meter
        self.battery_level_pct = max(0.0, self.battery_level_pct - energy_cost)
        logger.debug(f"{self.rover_id}: moved to {self.position} battery={self.battery_level_pct:.2f}%")

    def _perform_sampling(self, dt_s: float) -> None:
        """
        Simula perfuração/amostragem — cada sample leva uma certa duração.
        """
        params = self.current_mission.get("params", {})
        sample_count = int(params.get("sample_count", 1))
        # simple: collect one sample per call and consume energy
        if self.samples_collected < sample_count:
            # chance of sample failure
            if random.random() < 1 - math.pow((1 - self.p_sample_fail), dt_s):
                self._set_error("E-SAMPLE-01", "sampling", "Sampling tool failure")
                return
            # collect one sample
            self.samples_collected += 1
            self.payload_capacity_pct = max(0.0, self.payload_capacity_pct - (100.0 / max(1, sample_count)))
            # energy cost fixed per sample
            self.battery_level_pct = max(0.0, self.battery_level_pct - 2.0)
            logger.info(f"{self.rover_id}: collected sample {self.samples_collected}/{sample_count} battery={self.battery_level_pct:.1f}%")
            # simulate some time cost by leaving state as SAMPLING; caller should call step later
        else:
            # finished sampling
            self.mission_progress_pct = 100.0
            self.state = "COMPLETED"
            logger.info(f"{self.rover_id}: sampling mission completed")

    def _perform_capture(self, dt_s: float) -> None:
        """
        Simula captura de imagens — frames progress increment.
        """
        params = self.current_mission.get("params", {})
        frames = int(params.get("frames", 10))
        # approximate frames captured per dt_s based on small rate
        rate = max(1.0, frames / max(1.0, params.get("duration_s", frames)))  # fallback
        # capture a few frames
        frames_done = int(min(frames, self.current_mission.get("params", {}).get("_frames_done", 0) + max(1, int(rate * dt_s))))
        self.current_mission["params"]["_frames_done"] = frames_done
        # energy cost per frame
        self.battery_level_pct = max(0.0, self.battery_level_pct - 0.1 * (frames_done - self.current_mission["params"].get("_frames_prev", 0)))
        self.current_mission["params"]["_frames_prev"] = frames_done
        logger.debug(f"{self.rover_id}: captured frames {frames_done}/{frames} battery={self.battery_level_pct:.2f}%")
        if frames_done >= frames:
            self.mission_progress_pct = 100.0
            self.state = "COMPLETED"
            logger.info(f"{self.rover_id}: imaging mission completed")

    def _perform_analysis(self, dt_s: float) -> None:
        """
        Simula análise ambiental — acumula progress proporcional ao sampling_rate.
        """
        params = self.current_mission.get("params", {})
        sampling_rate = float(params.get("sampling_rate_s", 10.0))
        # progress increment per dt_s: dt_s / (expected_duration)
        expected_duration = params.get("expected_duration_s", sampling_rate * 5.0)
        inc = (dt_s / max(1.0, expected_duration)) * 100.0
        self.mission_progress_pct = min(100.0, self.mission_progress_pct + inc)
        # energy cost small
        self.battery_level_pct = max(0.0, self.battery_level_pct - 0.01 * dt_s)
        logger.debug(f"{self.rover_id}: analysis progress {self.mission_progress_pct:.2f}% battery={self.battery_level_pct:.2f}%")
        if self.mission_progress_pct >= 100.0:
            self.state = "COMPLETED"
            logger.info(f"{self.rover_id}: analysis mission completed")

    def _set_error(self, code: str, category: str, description: str) -> None:
        self.last_error = {"code": code, "category": category, "description": description, "timestamp": utils.now_iso()}
        self.state = "ERROR"
        logger.warning(f"{self.rover_id}: ERROR {code} - {description}")

    def inject_failure(self, kind: str) -> None:
        """
        Forçar uma falha específica: kind in {"locomotion","sampling","system","battery"}.
        """
        if kind == "locomotion":
            self._set_error("E-LOC-01", "locomotion", "Forced locomotion failure")
        elif kind == "sampling":
            self._set_error("E-SAMPLE-01", "sampling", "Forced sampling failure")
        elif kind == "system":
            self._set_error("E-SYS-01", "system", "Forced system failure")
        elif kind == "battery":
            self.battery_level_pct = 0.0
            self._set_error("E-BATT-LOW", "battery", "Forced battery drain")
        else:
            logger.warning("Unknown failure kind requested")

    def is_mission_complete(self) -> bool:
        return self.state == "COMPLETED"

    def get_telemetry(self) -> Dict[str, Any]:
        """
        Retorna um dicionário com os campos sugeridos para PROGRESS / TELEMETRY bodies.
        """
        with self._lock:
            telemetry = {
                "position": {"x": round(self.position[0], 3), "y": round(self.position[1], 3), "z": round(self.position[2], 3)},
                "battery_level_pct": round(self.battery_level_pct, 2),
                "payload_capacity_pct": round(self.payload_capacity_pct, 2),
                "mission_id": self.current_mission.get("mission_id") if self.current_mission else None,
                "progress_pct": round(self.mission_progress_pct, 2),
                "status": self.state.lower(),
                "samples_collected": self.samples_collected,
                "last_error": self.last_error,
            }
            return telemetry

    # --- Simple demo / standalone runner ---------------------------------
def _demo_run():
    rs = RoverSim("R-001", position=(0.0, 0.0, 0.0))
    mission = {
        "mission_id": "M-DEM0",
        "area": {"x1": 5, "y1": -2, "z1": 0, "x2": 10, "y2": 3, "z2": 0},
        "task": "collect_samples",
        "params": {"sample_count": 3},
        "update_interval_s": 2,
    }
    rs.start_mission(mission)
    start = time.time()
    try:
        while not rs.is_mission_complete() and rs.state != "ERROR" and time.time() - start < 600:
            rs.step(1.0)
            tel = rs.get_telemetry()
            logger.info(f"TELEMETRY: {tel}")
            time.sleep(1.0)
        logger.info("Demo finished. final telemetry:")
        logger.info(rs.get_telemetry())
    except KeyboardInterrupt:
        logger.info("Demo interrupted")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RoverSim demo runner")
    parser.add_argument("--demo", action="store_true", help="Run demo mission locally")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    if args.demo:
        _demo_run()
    else:
        print("Run with --demo to execute a sample mission")