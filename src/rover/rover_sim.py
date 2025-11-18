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
    def __init__(self, rover_id: str, position: Tuple[float, float, float] = (0.0, 0.0, 0.0)):
        self.rover_id = rover_id
        self.position = {"x": float(position[0]), "y": float(position[1]), "z": float(position[2])}
        self.state = "IDLE"  # IDLE, RUNNING, COMPLETED, ERROR
        self.current_mission: Optional[Dict[str, Any]] = None
        self.progress_pct: float = 0.0
        self.samples_collected: int = 0
        self.battery_level_pct: float = 100.0
        self.errors: list = []   # list of error dicts, empty when none
        self._elapsed_on_mission: float = 0.0
        self._mission_total_time: float = 10.0  # seconds, default short duration for tests

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
        Update progress, position (simple random walk), battery and possibly inject simple failures.
        """
        if self.state != "RUNNING" or not self.current_mission:
            return

        # advance internal timer
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
            return

        # Completion check
        if self._elapsed_on_mission >= self._mission_total_time or self.progress_pct >= 100.0:
            self.progress_pct = 100.0
            self.state = "COMPLETED"
            # Ensure samples_collected final values
            if task == "collect_samples":
                self.samples_collected = int(params.get("sample_count", 1))
            elif task == "capture_images":
                self.samples_collected = int(params.get("frames", 1))
            else:
                self.samples_collected = max(1, int(self.samples_collected))

    def is_mission_complete(self) -> bool:
        return self.state == "COMPLETED"

    def get_telemetry(self) -> Dict[str, Any]:
        """
        Return a telemetry dict used by ml_client to build PROGRESS/MISSION_COMPLETE bodies.

        The dict matches the canonical fields expected by the binary TLV packers / MissionStore:
          - mission_id, progress_pct, position, battery_level_pct, status, errors, samples_collected, timestamp_ms
        """
        ts_ms = int(time.time() * 1000)
        return {
            "mission_id": (self.current_mission.get("mission_id") if self.current_mission else None),
            "progress_pct": self.progress_pct,
            "position": dict(self.position),
            "battery_level_pct": round(self.battery_level_pct, 1),
            "status": self.state.lower(),
            "errors": list(self.errors),
            "samples_collected": int(self.samples_collected),
            "timestamp_ms": ts_ms,
        }