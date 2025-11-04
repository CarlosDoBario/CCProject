"""
mission_store.py
Missões em memória e armazenamento dos rovers usados pelo ML server (Nave-Mãe).

Responsabilidades:
- Gerir missões: criar, listar, atribuir, atualizar progresso, completar, cancelar.
- Gerir informação básica dos rovers: registo, estado, último contacto.
- Fornecer hooks/callbacks para notificações (ex.: API / SSE).
- Persistência leve opcional (salvar/load em ficheiro JSON).

Nota: esta implementação é simples, suficiente para prototipagem e testes.
Pode ser substituída por persistência mais completa (SQLite, etc.) se necessário.
"""

from typing import Dict, Any, Optional, Tuple, Callable, List
import json
import os
from threading import Lock

from common import utils

logger = utils.get_logger("ml.mission_store")


def _normalize_area(area: Optional[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    """
    Normaliza uma área para sempre conter x1,y1,z1,x2,y2,z2 como floats.
    - Se area for None retorna None.
    - Se a área só tiver x1,y1,x2,y2 assume z1=z2=0.0 (compatibilidade).
    - Garante que x1<=x2, y1<=y2, z1<=z2 (ordenando os valores).
    """
    if area is None:
        return None
    # copy to avoid mutating caller
    a = dict(area)
    # required: x1,y1,x2,y2 (z optional)
    try:
        x1 = float(a.get("x1"))
        y1 = float(a.get("y1"))
        x2 = float(a.get("x2"))
        y2 = float(a.get("y2"))
    except Exception:
        raise ValueError("Area must include x1,y1,x2,y2 numeric values")

    z1 = float(a.get("z1", 0.0))
    z2 = float(a.get("z2", 0.0))

    # Ensure ordering (x1<=x2 etc.)
    x_low, x_high = (min(x1, x2), max(x1, x2))
    y_low, y_high = (min(y1, y2), max(y1, y2))
    z_low, z_high = (min(z1, z2), max(z1, z2))

    return {
        "x1": x_low,
        "y1": y_low,
        "z1": z_low,
        "x2": x_high,
        "y2": y_high,
        "z2": z_high,
    }


class MissionStore:
    """
    In-memory store.
    API pública resumida:
      - register_rover(rover_id, address)
      - create_mission(mission_spec) -> mission_id
      - get_pending_mission_for_rover(rover_id) -> mission dict | None
      - assign_mission_to_rover(mission, rover_id)
      - update_progress(mission_id, rover_id, progress_payload)
      - complete_mission(mission_id, rover_id, result_payload)
      - cancel_mission(mission_id, reason)
      - list_missions()
      - get_rover(rover_id)
      - snapshot() -> dict (para API/inspeção)
      - load_from_file(path), save_to_file(path)
    """

    def __init__(self, persist_file: Optional[str] = None):
        self._lock = Lock()
        self._missions: Dict[str, Dict[str, Any]] = {}
        self._rovers: Dict[str, Dict[str, Any]] = {}
        self._next_mission_seq = 1
        self.persist_file = persist_file
        # callback hooks list: functions called on changes: fn(event_type, payload)
        self._hooks: List[Callable[[str, Dict[str, Any]], None]] = []

        # load persisted if present
        if self.persist_file and os.path.exists(self.persist_file):
            try:
                self.load_from_file(self.persist_file)
                logger.info(f"Loaded mission_store from {self.persist_file}")
            except Exception:
                logger.exception("Failed to load persisted mission_store")

    # --- Hooks ---------------------------------------------------------------
    def register_hook(self, fn: Callable[[str, Dict[str, Any]], None]) -> None:
        """Regista um callback que será chamado em eventos (mission_assigned, progress, complete, cancel)."""
        self._hooks.append(fn)

    def _emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        for fn in self._hooks:
            try:
                fn(event_type, payload)
            except Exception:
                logger.exception("hook error")

    # --- Rover management ---------------------------------------------------
    def register_rover(self, rover_id: str, address: Optional[Tuple[str, int]] = None) -> None:
        """Regista/update info básica do rover (address é opcional)."""
        with self._lock:
            r = self._rovers.setdefault(rover_id, {})
            r["rover_id"] = rover_id
            if address:
                r["address"] = {"ip": address[0], "port": address[1]}
            r["last_seen"] = utils.now_iso()
            r.setdefault("state", "IDLE")
        logger.debug(f"Rover registered/updated: {rover_id}")

    def get_rover(self, rover_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._rovers.get(rover_id)

    def list_rovers(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._rovers)

    # --- Mission lifecycle -------------------------------------------------
    def create_mission(self, mission_spec: Dict[str, Any]) -> str:
        """
        Cria missão a partir de mission_spec (espera campos como task, area, params, priority, etc.)
        Gera mission_id se não for fornecido.
        Normaliza a área para incluir z1,z2.
        """
        with self._lock:
            if mission_spec.get("mission_id"):
                mid = mission_spec["mission_id"]
                if mid in self._missions:
                    raise ValueError("mission_id already exists")
            else:
                mid = f"M-{self._next_mission_seq:04d}"
                self._next_mission_seq += 1

            area_norm = None
            if "area" in mission_spec and mission_spec["area"] is not None:
                area_norm = _normalize_area(mission_spec["area"])

            m = {
                "mission_id": mid,
                "task": mission_spec.get("task"),
                "area": area_norm,
                "params": mission_spec.get("params", {}),
                "priority": mission_spec.get("priority", 1),
                "max_duration_s": mission_spec.get("max_duration_s"),
                "update_interval_s": mission_spec.get("update_interval_s"),
                "created_at": utils.now_iso(),
                "state": "CREATED",
                "assigned_rover": None,
                "assigned_at": None,
                "history": [],
            }
            self._missions[mid] = m
        logger.info(f"Created mission {mid} task={m['task']}")
        self._emit("mission_created", {"mission": m.copy()})
        return mid

    def get_mission(self, mission_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            m = self._missions.get(mission_id)
            return dict(m) if m else None

    def list_missions(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {mid: dict(m) for mid, m in self._missions.items()}

    def get_pending_mission_for_rover(self, rover_id: str) -> Optional[Dict[str, Any]]:
        """
        Política simples: retorna a missão de maior prioridade não atribuída.
        """
        with self._lock:
            pending = [m for m in self._missions.values() if m["assigned_rover"] is None]
            if not pending:
                return None
            # escolher por prioridade ascendente (1 = alta prioridade se desejado) — aqui escolhemos maior priority value
            pending.sort(key=lambda x: (-int(x.get("priority", 1)), x.get("created_at")))
            # return a copy to avoid external mutation
            return dict(pending[0])

    def assign_mission_to_rover(self, mission: Dict[str, Any], rover_id: str) -> None:
        with self._lock:
            mid = mission["mission_id"]
            if mid not in self._missions:
                raise KeyError("mission not found")
            m = self._missions[mid]
            if m["assigned_rover"] is not None:
                raise ValueError("mission already assigned")
            m["assigned_rover"] = rover_id
            m["state"] = "ASSIGNED"
            m["assigned_at"] = utils.now_iso()
            self._rovers.setdefault(rover_id, {})["state"] = "ASSIGNED"
            # record history
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "ASSIGNED", "rover": rover_id})
        logger.info(f"Mission {mid} assigned to {rover_id}")
        self._emit("mission_assigned", {"mission": dict(m), "rover_id": rover_id})

    def update_progress(self, mission_id: str, rover_id: str, progress_payload: Dict[str, Any]) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"update_progress: unknown mission {mission_id}")
                return
            entry = {"ts": utils.now_iso(), "type": "PROGRESS", "rover": rover_id, "payload": progress_payload}
            m.setdefault("history", []).append(entry)
            # update summary fields
            m["state"] = "IN_PROGRESS"
            m["last_progress_pct"] = progress_payload.get("progress_pct")
            # update rover state
            self._rovers.setdefault(rover_id, {})["state"] = "IN_MISSION"
        logger.debug(f"Progress updated for mission {mission_id} by {rover_id}: {progress_payload.get('progress_pct')}")
        self._emit("mission_progress", {"mission_id": mission_id, "rover_id": rover_id, "progress": progress_payload})

    def complete_mission(self, mission_id: str, rover_id: str, result_payload: Dict[str, Any]) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"complete_mission: unknown mission {mission_id}")
                return
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "COMPLETE", "rover": rover_id, "payload": result_payload})
            m["state"] = "COMPLETED"
            m["completed_at"] = utils.now_iso()
            self._rovers.setdefault(rover_id, {})["state"] = "IDLE"
        logger.info(f"Mission {mission_id} completed by {rover_id}")
        self._emit("mission_complete", {"mission_id": mission_id, "rover_id": rover_id, "result": result_payload})

    def cancel_mission(self, mission_id: str, reason: Optional[str] = None) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"cancel_mission: unknown mission {mission_id}")
                return
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL", "reason": reason})
            m["state"] = "CANCELLED"
            m["cancelled_at"] = utils.now_iso()
            assigned = m.get("assigned_rover")
            if assigned:
                self._rovers.setdefault(assigned, {})["state"] = "IDLE"
        logger.info(f"Mission {mission_id} cancelled: {reason}")
        self._emit("mission_cancel", {"mission_id": mission_id, "reason": reason})

    # --- Persistence --------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {"missions": {k: dict(v) for k, v in self._missions.items()}, "rovers": {k: dict(v) for k, v in self._rovers.items()}}

    def save_to_file(self, path: Optional[str] = None) -> None:
        p = path or self.persist_file
        if not p:
            raise ValueError("No persist file configured")
        with self._lock:
            payload = self.snapshot()
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info(f"Mission store saved to {p}")

    def load_from_file(self, path: Optional[str] = None) -> None:
        p = path or self.persist_file
        if not p or not os.path.exists(p):
            raise ValueError("Persist file not found")
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
        with self._lock:
            self._missions = payload.get("missions", {})
            self._rovers = payload.get("rovers", {})
            # recompute next sequence
            seqs = []
            for mid in self._missions.keys():
                try:
                    seqs.append(int(mid.split("-")[-1]))
                except Exception:
                    continue
            self._next_mission_seq = max(seqs) + 1 if seqs else 1
        logger.info(f"Mission store loaded from {p}")

    # --- Convenience --------------------------------------------------------
    def create_demo_missions(self) -> None:
        """Cria algumas missões de exemplo para testes rápidos (áreas 3D com z)."""
        demos = [
            {"task": "capture_images", "area": {"x1": 10, "y1": 10, "z1": 0, "x2": 20, "y2": 20, "z2": 0}, "params": {"interval_s": 5, "frames": 60}, "priority": 1},
            {"task": "collect_samples", "area": {"x1": 30, "y1": 5, "z1": 0, "x2": 35, "y2": 10, "z2": 0}, "params": {"depth_mm": 50, "sample_count": 2}, "priority": 2},
            {"task": "env_analysis", "area": {"x1": 0, "y1": 0, "z1": 0, "x2": 50, "y2": 50, "z2": 5}, "params": {"sensors": ["temp", "pressure"], "sampling_rate_s": 10}, "priority": 3},
        ]
        for m in demos:
            try:
                self.create_mission(m)
            except Exception:
                logger.exception("create_demo_missions: failed to create demo mission")