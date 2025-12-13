from typing import Dict, Any, Optional, List, Callable
import time
from threading import Lock

from common import utils

logger = utils.get_logger("nave_mae.telemetry_store")


class TelemetryStore:
    def __init__(self, mission_store: Optional[Any] = None):
        self._lock = Lock()
        # rover_id -> {last_telemetry: dict, last_update_ms: int, ...}
        self._rovers: Dict[str, Dict[str, Any]] = {}
        self.mission_store = mission_store
        self._hooks: List[Callable[[str, Dict[str, Any]], None]] = []

    def register_hook(self, fn: Callable[[str, Dict[str, Any]], None]) -> None:
        self._hooks.append(fn)

    def _emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        for fn in list(self._hooks):
            try:
                fn(event_type, payload)
            except Exception:
                logger.exception("Telemetry hook error")

    def register_rover(self, rover_id: str, address: Optional[Any] = None) -> None:
        with self._lock:
            r = self._rovers.setdefault(rover_id, {"rover_id": rover_id, "last_update_ms": 0, "last_telemetry": {}})
            if address:
                r["address"] = address
        logger.debug(f"TelemetryStore registered/updated rover: {rover_id}")

    def update(self, rover_id: str, telemetry: Dict[str, Any]) -> None:
        now_ms = utils.now_ms()
        
        # Copia todos os campos da telemetria canónica para o armazenamento
        # Garante que todos os novos campos estão incluídos
        update_data = {
            "last_telemetry": dict(telemetry),
            "last_update_ms": now_ms,
            "status": telemetry.get("status", "UNKNOWN"),
            "progress_pct": telemetry.get("progress_pct"), # Se progress_pct for reintroduzido no TS
            "battery_level_pct": telemetry.get("battery_level_pct"),
            "position": telemetry.get("position"),
            "internal_temp_c": telemetry.get("internal_temp_c"),
            "current_speed_m_s": telemetry.get("current_speed_m_s"),
        }

        with self._lock:
            # Garante que a entrada do rover existe
            rover_entry = self._rovers.setdefault(rover_id, {"rover_id": rover_id, "last_update_ms": 0, "last_telemetry": {}})
            
            # Atualiza a entrada do rover com os dados mais recentes
            rover_entry.update(update_data)
        
        logger.debug(f"Telemetry updated for {rover_id}. Status: {update_data['status']}")      
        self._emit("telemetry_update", {"rover_id": rover_id, "telemetry": telemetry})
        if self.mission_store and hasattr(self.mission_store, "update_from_telemetry"):
            try:
                self.mission_store.update_from_telemetry(rover_id, telemetry)
            except Exception:
                logger.exception("Failed to update MissionStore from TelemetryStore hook")

    def get_latest(self, rover_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._rovers.get(rover_id)
            return dict(entry.get("last_telemetry")) if entry and entry.get("last_telemetry") else None

    def get_rover_state(self, rover_id: str) -> Optional[Dict[str, Any]]:
        """Return aggregated state (last telemetry + metadata)."""
        with self._lock:
            entry = self._rovers.get(rover_id)
            return dict(entry) if entry else None

    def list_rovers_latest_state(self) -> Dict[str, Dict[str, Any]]:
        """Return map of rover_id to latest aggregated state."""
        with self._lock:
            return {k: dict(v) for k, v in self._rovers.items()}
