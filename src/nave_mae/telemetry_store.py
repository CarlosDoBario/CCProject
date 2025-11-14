"""
TelemetryStore for the Nave-Mãe.

This module implements an asyncio-friendly in-memory store that:
- keeps the latest telemetry per rover,
- retains a bounded history per rover,
- exposes async API to update/query telemetry,
- allows registering hooks to notify other components (MissionStore) on new telemetry.

Placement: src/nave_mae/telemetry_store.py (fits the existing repository layout).
"""

from typing import Any, Dict, List, Callable, Optional
import asyncio
from collections import deque, defaultdict
import copy
from datetime import datetime, timezone
import logging

_logger = logging.getLogger("nave_mae.telemetry_store")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelemetryStore:
    """
    Async telemetry store.

    Usage:
        store = TelemetryStore(history_size=200)
        await store.update("R-1", {"position": {"x":1,"y":2}, "state": "IN_MISSION", "battery_pct": 88})
        latest = await store.get_latest("R-1")
    """

    def __init__(self, history_size: int = 100):
        # latest telemetry per rover_id
        self._latest: Dict[str, Dict[str, Any]] = {}
        # history per rover_id (deque)
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        # asyncio lock for concurrent access from multiple coroutines
        self._lock = asyncio.Lock()
        # hooks to call on new telemetry: fn(event_type: str, payload: dict)
        self._hooks: List[Callable[[str, Dict[str, Any]], Any]] = []

    def register_hook(self, fn: Callable[[str, Dict[str, Any]], Any]) -> None:
        """
        Register a hook called on each telemetry update.
        Hook signature: fn(event_type: str, payload: dict).
        Hook may be synchronous or async (coroutine function).
        """
        self._hooks.append(fn)
        _logger.debug("TelemetryStore: registered hook %s", getattr(fn, "__name__", repr(fn)))

    async def _call_hooks(self, event_type: str, payload: Dict[str, Any]) -> None:
        """
        Call registered hooks. Async hooks are scheduled (create_task) so we don't block updates.
        Synchronous hooks are called directly (should be fast).
        """
        for fn in list(self._hooks):
            try:
                if asyncio.iscoroutinefunction(fn):
                    asyncio.create_task(fn(event_type, payload))
                else:
                    try:
                        fn(event_type, payload)
                    except Exception:
                        _logger.exception("TelemetryStore hook (sync) raised")
            except Exception:
                _logger.exception("Failed to schedule telemetry hook")

    async def update(self, rover_id: str, telemetry: Dict[str, Any]) -> None:
        """
        Record a telemetry sample for rover_id.
        Ensures telemetry contains rover_id and ts (ISO timestamp).
        Calls hooks (non-blocking).
        """
        if not isinstance(telemetry, dict):
            raise ValueError("telemetry must be a dict")

        # shallow copy and ensure fields
        telemetry = dict(telemetry)
        telemetry.setdefault("rover_id", rover_id)
        telemetry.setdefault("ts", now_iso())

        async with self._lock:
            # deep copy to avoid external mutation
            self._latest[rover_id] = copy.deepcopy(telemetry)
            self._history[rover_id].append(copy.deepcopy(telemetry))
            _logger.debug("TelemetryStore: update rover=%s ts=%s", rover_id, telemetry.get("ts"))

        # notify hooks outside the lock
        try:
            await self._call_hooks("telemetry", {"rover_id": rover_id, "telemetry": telemetry})
        except Exception:
            _logger.exception("TelemetryStore: hooks raised during update")

    async def get_latest(self, rover_id: str) -> Optional[Dict[str, Any]]:
        """Return a deep copy of the latest telemetry for rover_id, or None if unknown."""
        async with self._lock:
            t = self._latest.get(rover_id)
            return copy.deepcopy(t) if t is not None else None

    async def get_history(self, rover_id: str, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return up to n most recent telemetry entries for rover_id.
        If n is None, return the entire retained history (bounded by history_size).
        Results ordered oldest -> newest.
        """
        async with self._lock:
            hist = list(self._history.get(rover_id, []))
            if n is None:
                return copy.deepcopy(hist)
            return copy.deepcopy(hist[-n:])

    async def list_rovers(self) -> List[str]:
        """Return the list of known rover IDs (those with any telemetry)."""
        async with self._lock:
            return list(self._latest.keys())

    async def snapshot(self) -> Dict[str, Any]:
        """Return a snapshot (rover_id -> latest telemetry) deep-copied."""
        async with self._lock:
            return {rid: copy.deepcopy(t) for rid, t in self._latest.items()}

    async def clear(self) -> None:
        """Clear all stored telemetry (useful in tests)."""
        async with self._lock:
            self._latest.clear()
            self._history.clear()
            _logger.debug("TelemetryStore: cleared")