#!/usr/bin/env python3
"""
mission_store.py

In-memory MissionStore with optional persistence and basic recovery.

Features:
- create/assign/update/complete/cancel/unassign mission APIs (same surface as before)
- register_hook(fn) to receive events (fn(event_type, payload))
- persist to JSON file on state-changing operations (atomic write)
- load from JSON on init and perform basic recovery:
    - missions in ASSIGNED or IN_PROGRESS are reverted to CREATED and history records RECOVERED
    - emits hooks for recovered missions
- constructor accepts persist_file (or reads ML_MISSION_STORE_FILE env var)
"""

from typing import Dict, Any, Optional, Tuple, Callable, List
import json
import os
from threading import Lock
import tempfile
import shutil
import time

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


class MissionStore:
    """
    MissionStore with persistence and basic recovery.
    """

    def __init__(self, persist_file: Optional[str] = None):
        # allow env var override if not provided
        self.persist_file = persist_file or os.getenv("ML_MISSION_STORE_FILE")
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
        with self._lock:
            r = self._rovers.setdefault(rover_id, {})
            r["rover_id"] = rover_id
            if address:
                r["address"] = {"ip": address[0], "port": address[1]}
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
            return dict(self._rovers)

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

    def get_pending_mission_for_rover(self, rover_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            pending = [m for m in self._missions.values() if m["assigned_rover"] is None]
            if not pending:
                return None
            pending.sort(key=lambda x: (-int(x.get("priority", 1)), x.get("created_at")))
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

    # --- Convenience / persistence utilities -------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {"missions": {k: dict(v) for k, v in self._missions.items()}, "rovers": {k: dict(v) for k, v in self._rovers.items()}, "_next_mission_seq": self._next_mission_seq}

    def save_snapshot_to(self, path: str) -> None:
        """Public helper to force-save to a custom path."""
        self.save_to_file(path)

    def create_demo_missions(self) -> None:
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