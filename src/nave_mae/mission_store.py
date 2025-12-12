#!/usr/bin/env python3
"""
mission_store.py

In-memory MissionStore with optional persistence and basic recovery.
"""

from typing import Dict, Any, Optional, Tuple, Callable, List
import json
import os
from threading import Lock
import tempfile
import time
from datetime import datetime, timezone

from common import utils, mission_schema

logger = utils.get_logger("ml.mission_store")


def _normalize_area(area: Optional[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    if area is None:
        return None
    a = dict(area)
    try:
        x1 = float(a.get("x1"))
        y1 = float(a.get("y1"))
        x2 = float(a.get("x2"))
        y2 = float(a.get("y2"))
    except Exception:
        raise ValueError("Area must include x1,y1,x2,y2 numeric values")
    z1 = float(a.get("z1", 0.0))
    z2 = float(a.get("z2", 0.0))
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


def _iso_from_ms(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return utils.now_iso()


class MissionStore:
    """
    MissionStore with persistence and basic recovery.
    """

    def __init__(self, persist_file: Optional[str] = None):
        # allow env var override if not provided
        from common import config
        self.persist_file = persist_file or config.MISSION_STORE_FILE
        self._lock = Lock()
        self._missions: Dict[str, Dict[str, Any]] = {}
        self._rovers: Dict[str, Dict[str, Any]] = {}
        self._next_mission_seq = 1
        self._hooks: List[Callable[[str, Dict[str, Any]], None]] = []

        # If persist_file exists, attempt to load and recover
        if self.persist_file and os.path.exists(self.persist_file):
            try:
                self.load_from_file(self.persist_file)
                logger.info(f"Loaded mission_store from {self.persist_file}")
                # perform recovery of in-progress/assigned missions
                self._recover_on_startup()
            except Exception:
                logger.exception("Failed to load/ recover mission_store")

    # --- Hooks ---------------------------------------------------------------
    def register_hook(self, fn: Callable[[str, Dict[str, Any]], None]) -> None:
        self._hooks.append(fn)

    def _emit(self, event_type: str, payload: Dict[str, Any]) -> None:
        for fn in list(self._hooks):
            try:
                fn(event_type, payload)
            except Exception:
                logger.exception("hook error")

    # --- Persistence helpers -----------------------------------------------
    def _atomic_write(self, path: str, data: str) -> None:
        """Write data to path atomically (tmp + rename)."""
        dirn = os.path.dirname(path) or "."
        fd, tmppath = tempfile.mkstemp(prefix="ms-", dir=dirn, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            # os.replace is atomic on most platforms
            os.replace(tmppath, path)
        except Exception:
            try:
                os.remove(tmppath)
            except Exception:
                pass
            raise

    def save_to_file(self, path: Optional[str] = None) -> None:
        p = path or self.persist_file
        if not p:
            raise ValueError("No persist file configured")
        with self._lock:
            payload = {"missions": self._missions, "rovers": self._rovers, "_next_mission_seq": self._next_mission_seq}
            data = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        try:
            self._atomic_write(p, data)
            logger.info(f"Mission store saved to {p}")
        except Exception:
            logger.exception(f"Failed to persist mission store to {p}")

    def load_from_file(self, path: Optional[str] = None) -> None:
        p = path or self.persist_file
        if not p or not os.path.exists(p):
            raise ValueError("Persist file not found")
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
        with self._lock:
            self._missions = payload.get("missions", {})
            self._rovers = payload.get("rovers", {})
            self._next_mission_seq = payload.get("_next_mission_seq", 1)

    # recovery: adjust missions that were in transient states
    def _recover_on_startup(self) -> None:
        recovered = []
        with self._lock:
            for mid, m in list(self._missions.items()):
                state = m.get("state")
                if state in ("ASSIGNED", "IN_PROGRESS"):
                    prev_rover = m.get("assigned_rover")
                    m["assigned_rover"] = None
                    # Use a clear recovery state in history; keep state as CREATED so it's reapplied
                    m["state"] = "CREATED"
                    m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "RECOVERED", "prev_rover": prev_rover})
                    recovered.append({"mission_id": mid, "prev_rover": prev_rover})
                    # update rover state if present
                    if prev_rover:
                        self._rovers.setdefault(prev_rover, {})["state"] = "IDLE"
        if recovered:
            logger.info(f"Recovered {len(recovered)} mission(s) on startup: {recovered}")
            # emit hooks for each recovered mission
            for r in recovered:
                try:
                    self._emit("mission_recovered", {"mission_id": r["mission_id"], "prev_rover": r["prev_rover"]})
                except Exception:
                    logger.exception("emit mission_recovered failed")
            # persist the cleaned state immediately
            try:
                if self.persist_file:
                    self.save_to_file()
            except Exception:
                logger.exception("Failed to save mission_store after recovery")

    # --- Rover management ---------------------------------------------------
    def register_rover(self, rover_id: str, address: Optional[Tuple[str, int]] = None) -> None:
        """
        Register or update a rover entry. `address` can be:
          - tuple (ip, port)
          - dict {"ip":..., "port":...}
          - None
        """
        with self._lock:
            r = self._rovers.setdefault(rover_id, {})
            r["rover_id"] = rover_id
            if address:
                if isinstance(address, dict):
                    # accept dict form
                    ip = address.get("ip")
                    port = address.get("port")
                    if ip:
                        r["address"] = {"ip": ip, "port": int(port) if port is not None else port}
                elif isinstance(address, (tuple, list)) and len(address) >= 2:
                    r["address"] = {"ip": address[0], "port": int(address[1])}
                else:
                    # unknown form: store as raw
                    r["address"] = address
            r["last_seen"] = utils.now_iso()
            r.setdefault("state", "IDLE")
        logger.debug(f"Rover registered/updated: {rover_id}")
        self._emit("rover_registered", {"rover_id": rover_id, "address": r.get("address")})
        # persist
        if self.persist_file:
            self.save_to_file()

    def get_rover(self, rover_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            r = self._rovers.get(rover_id)
            return dict(r) if r else None

    def list_rovers(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            # return shallow copy
            return {k: dict(v) for k, v in self._rovers.items()}

    # --- Mission lifecycle -------------------------------------------------
    def create_mission(self, mission_spec: Dict[str, Any]) -> str:
        ok, errors = mission_schema.validate_mission_spec(mission_spec)
        if not ok:
            raise ValueError(f"Invalid mission_spec: {errors}")

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
        self._emit("mission_created", {"mission": dict(m)})
        # persist
        if self.persist_file:
            self.save_to_file()
        return mid

    def get_mission(self, mission_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            m = self._missions.get(mission_id)
            return dict(m) if m else None

    def list_missions(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {mid: dict(m) for mid, m in self._missions.items()}

    def get_pending_mission_for_rover(self, rover_id: str, mission_id_hint: Optional[str] = None) -> Optional[Dict[str, Any]]: 
        """
        Retorna a próxima missão para o rover. Prioriza a missão solicitada para retoma, 
        se estiver atribuída ao rover e não estiver concluída/cancelada.
        """
        with self._lock:
            # 1. Trata a Dica de Retomada de Missão (Resumption Hint)
            if mission_id_hint:
                m_hint = self._missions.get(mission_id_hint)
                if m_hint:
                    # Se a missão estiver atribuída a este rover e não estiver concluída/cancelada, 
                    # significa que foi interrompida e o rover está a pedir para a retomar. Priorizar!
                    if m_hint["assigned_rover"] == rover_id and m_hint["state"] in ("ASSIGNED", "IN_PROGRESS"):
                        logger.info(f"Prioritizing assigned mission {mission_id_hint} for resumption by {rover_id}.")
                        return dict(m_hint)
                    
                    # Se a missão estiver em estado CREATED mas for solicitada explicitamente (para garantir a ordem), atribuí-la.
                    if m_hint["assigned_rover"] is None and m_hint["state"] == "CREATED":
                         return dict(m_hint)
                         
                    # Se estiver atribuída a outro rover ou em estado final, cai no fallback.

            # 2. Fallback para a lógica de prioridade (apenas missões CREATED / não atribuídas)
            pending = [m for m in self._missions.values() if m["assigned_rover"] is None and m["state"] == "CREATED"]
            if not pending:
                return None
            
            # Ordena por prioridade (menor número = maior prioridade)
            # A prioridade é negativa porque a ordenação padrão é crescente (ascendente)
            pending.sort(key=lambda x: (-int(x.get("priority", 1)), x.get("created_at")))
            
            logger.info(f"Falling back to next CREATED mission: {pending[0].get('mission_id')} (Priority: {pending[0].get('priority')})")
            return dict(pending[0])


    def assign_mission_to_rover(self, mission: Dict[str, Any], rover_id: str) -> None:
        with self._lock:
            mid = mission["mission_id"]
            if mid not in self._missions:
                raise KeyError("mission not found")
            m = self._missions[mid]
            if m["assigned_rover"] is not None and m["assigned_rover"] != rover_id:
                raise ValueError(f"mission already assigned to {m['assigned_rover']}")
            m["assigned_rover"] = rover_id
            m["state"] = "ASSIGNED"
            m["assigned_at"] = utils.now_iso()
            self._rovers.setdefault(rover_id, {})["state"] = "ASSIGNED"
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "ASSIGNED", "rover": rover_id})
        logger.info(f"Mission {mid} assigned to {rover_id}")
        self._emit("mission_assigned", {"mission": dict(m), "rover_id": rover_id})
        if self.persist_file:
            self.save_to_file()

    def unassign_mission(self, mission_id: str, reason: Optional[str] = None) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"unassign_mission: unknown mission {mission_id}")
                return
            prev = m.get("assigned_rover")
            m["assigned_rover"] = None
            m["state"] = "CREATED"
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "ASSIGN_FAILED", "reason": reason})
            if prev:
                self._rovers.setdefault(prev, {})["state"] = "IDLE"
        logger.info(f"Mission {mission_id} unassigned (reason={reason})")
        self._emit("mission_assign_failed", {"mission_id": mission_id, "reason": reason, "prev_rover": prev})
        if self.persist_file:
            self.save_to_file()

    def update_progress(self, mission_id: str, rover_id: str, progress_payload: Dict[str, Any]) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"update_progress: unknown mission {mission_id}")
                return
            entry = {"ts": utils.now_iso(), "type": "PROGRESS", "rover": rover_id, "payload": progress_payload}
            m.setdefault("history", []).append(entry)
            m["state"] = "IN_PROGRESS"
            # store last_progress_pct only if present
            if progress_payload.get("progress_pct") is not None:
                try:
                    m["last_progress_pct"] = float(progress_payload.get("progress_pct"))
                except Exception:
                    m["last_progress_pct"] = progress_payload.get("progress_pct")
            self._rovers.setdefault(rover_id, {})["state"] = "IN_MISSION"
        logger.debug(f"Progress updated for mission {mission_id} by {rover_id}: {progress_payload.get('progress_pct')}")
        self._emit("mission_progress", {"mission_id": mission_id, "rover_id": rover_id, "progress": progress_payload})
        if self.persist_file:
            self.save_to_file()

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
        if self.persist_file:
            self.save_to_file()

    def cancel_mission(self, mission_id: str, reason: Optional[str] = None) -> None:
        with self._lock:
            m = self._missions.get(mission_id)
            if not m:
                logger.warning(f"cancel_mission: unknown mission {mission_id}")
                return
            prev = m.get("assigned_rover")
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL", "reason": reason})
            m["state"] = "CANCELLED"
            m["cancelled_at"] = utils.now_iso()
            if prev:
                self._rovers.setdefault(prev, {})["state"] = "IDLE"
        logger.info(f"Mission {mission_id} cancelled: {reason}")
        self._emit("mission_cancel", {"mission_id": mission_id, "reason": reason})
        if self.persist_file:
            self.save_to_file()

    # --- Telemetry integration helper ---------------------------------------
    def update_from_telemetry(self, rover_id: str, telemetry: Dict[str, Any]) -> None:
        """
        Integrate canonical telemetry payloads into MissionStore.
        """
        try:
            with self._lock:
                # ensure rover entry
                r = self._rovers.setdefault(rover_id, {"rover_id": rover_id, "state": "IDLE"})
                # address handling
                addr = telemetry.get("addr") or telemetry.get("_addr")
                if addr:
                    if isinstance(addr, dict):
                        ip = addr.get("ip")
                        port = addr.get("port")
                        if ip:
                            r["address"] = {"ip": ip, "port": int(port) if port is not None else port}
                    elif isinstance(addr, (tuple, list)) and len(addr) >= 2:
                        r["address"] = {"ip": addr[0], "port": int(addr[1])}
                    else:
                        r["address"] = addr
                # last_seen
                if telemetry.get("_ts_server_received_ms") is not None:
                    try:
                        r["last_seen"] = _iso_from_ms(int(telemetry.get("_ts_server_received_ms")))
                    except Exception:
                        r["last_seen"] = utils.now_iso()
                else:
                    r["last_seen"] = utils.now_iso()
                # update state from status if present
                status = telemetry.get("status")
                if status:
                    # store verbatim (string)
                    r["state"] = str(status)
                # persist rover change will be done after handling mission-specific updates

                # emit telemetry event (before possibly completing mission)
                self._emit("telemetry", {"rover_id": rover_id, "telemetry": dict(telemetry)})

                # forward mission progress if present
                mid = telemetry.get("mission_id")
                prog = telemetry.get("progress_pct")
                # treat progress numbers liberally: accept int/float/str
                prog_num = None
                if prog is not None:
                    try:
                        prog_num = float(prog)
                    except Exception:
                        prog_num = None

                if mid and prog is not None:
                    # call update_progress (which will persist)
                    try:
                        # pass the telemetry as the progress payload so history stores useful fields
                        self.update_progress(mid, rover_id, telemetry)
                    except Exception:
                        logger.exception("update_progress from telemetry failed")

                # check completion conditions: explicit status or progress >= 100
                completed = False
                if prog_num is not None and prog_num >= 100.0:
                    completed = True
                if isinstance(status, str) and status.lower() in ("completed", "complete", "success", "done"):
                    completed = True

                if completed and mid:
                    try:
                        self.complete_mission(mid, rover_id, telemetry)
                    except Exception:
                        logger.exception("complete_mission from telemetry failed")

            # persist after telemetry handling (persist inside save_to_file acquires lock internally)
            if self.persist_file:
                try:
                    self.save_to_file()
                except Exception:
                    logger.exception("Failed to save mission_store after telemetry update")
        except Exception:
            logger.exception("Unexpected error in update_from_telemetry")

    # --- Convenience / persistence utilities -------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {"missions": {k: dict(v) for k, v in self._missions.items()}, "rovers": {k: dict(v) for k, v in self._rovers.items()}, "_next_mission_seq": self._next_mission_seq}

    def save_snapshot_to(self, path: str) -> None:
        """Public helper to force-save to a custom path."""
        self.save_to_file(path)

    def create_demo_missions(self) -> None:
        """
        Cria as 3 missões de demonstração (M-001, M-002, M-003) com prioridades distintas.
        A ordem de atribuição será sempre P1 > P2 > P3.
        """
        demos = [
            # P1: Mais Alta Prioridade
            {"task": "capture_images", "mission_id": "M-001", "area": {"x1": 10, "y1": 10, "z1": 0, "x2": 20, "y2": 20, "z2": 0}, "params": {"interval_s": 5, "frames": 60}, "priority": 1, "max_duration_s": 20.0, "update_interval_s": 1.0},
            # P2: Média Prioridade
            {"task": "collect_samples", "mission_id": "M-002", "area": {"x1": 30, "y1": 5, "z1": 0, "x2": 35, "y2": 10, "z2": 0}, "params": {"depth_mm": 50, "sample_count": 2}, "priority": 2, "max_duration_s": 15.0, "update_interval_s": 1.0},
            # P3: Baixa Prioridade
            {"task": "env_analysis", "mission_id": "M-003", "area": {"x1": 85, "y1": 85, "z1": 0, "x2": 150, "y2": 150, "z2": 5}, "params": {"sensors": ["temp", "pressure"], "sampling_rate_s": 10}, "priority": 3, "max_duration_s": 20.0, "update_interval_s": 1.0},
        ]
        for m in demos:
            try:
                self.create_mission(m) 
            except Exception as e:
                logger.warning(f"create_demo_missions: failed to create demo mission {m.get('mission_id')}. Assuming it already exists or error: {e}")