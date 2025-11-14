"""
TelemetryStore for the Nave-Mãe.

Async, in-memory store that:
 - keeps the latest telemetry per rover,
 - retains a bounded history per rover,
 - exposes async API to update/query telemetry,
 - supports hooks to notify other components (e.g. MissionStore).

Place this file at: src/nave_mae/telemetry_store.py
"""

from typing import Any, Dict, List, Callable, Optional
import asyncio
import inspect
from collections import deque, defaultdict
import copy
from datetime import datetime, timezone
import logging

_logger = logging.getLogger("ml.telemetry_store")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TelemetryStore:
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
        Register a hook to be called on new telemetry updates.
        Hook signature: fn(event_type: str, payload: dict)
        Hook can be sync or async function.
        """
        self._hooks.append(fn)
        _logger.debug("Registered telemetry hook %s", getattr(fn, "__name__", repr(fn)))

    async def _call_hooks(self, event_type: str, payload: Dict[str, Any]) -> None:
        for fn in list(self._hooks):
            try:
                if inspect.iscoroutinefunction(fn):
                    # schedule and don't await to avoid blocking store updates
                    asyncio.create_task(fn(event_type, payload))
                else:
                    # call sync functions; keep them small/fast
                    try:
                        fn(event_type, payload)
                    except Exception:
                        _logger.exception("telemetry hook (sync) error")
            except Exception:
                _logger.exception("telemetry hook scheduling error")

    async def update(self, rover_id: str, telemetry: Dict[str, Any]) -> None:
        """
        Record a telemetry sample for rover_id.
        telemetry is expected to already contain timestamp 'ts' or else we add one.
        """
        if not isinstance(telemetry, dict):
            raise ValueError("telemetry must be a dict")
        # ensure rover_id inside telemetry for convenience
        telemetry = dict(telemetry)  # shallow copy
        telemetry.setdefault("rover_id", rover_id)
        telemetry.setdefault("ts", now_iso())

        async with self._lock:
            # store latest (deep copy to avoid external mutation)
            self._latest[rover_id] = copy.deepcopy(telemetry)
            # append to history
            self._history[rover_id].append(copy.deepcopy(telemetry))
            _logger.debug("Telemetry updated for %s ts=%s", rover_id, telemetry.get("ts"))

        # notify hooks (outside lock)
        try:
            await self._call_hooks("telemetry", {"rover_id": rover_id, "telemetry": telemetry})
        except Exception:
            _logger.exception("Error calling telemetry hooks")

    async def get_latest(self, rover_id: str) -> Optional[Dict[str, Any]]:
        """Return a deep copy of latest telemetry for rover_id or None."""
        async with self._lock:
            t = self._latest.get(rover_id)
            return copy.deepcopy(t) if t is not None else None

    async def get_history(self, rover_id: str, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return up to n most recent telemetry samples for rover_id.
        If n is None, return the entire retained history (bounded by history_size).
        Results are ordered oldest->newest.
        """
        async with self._lock:
            hist = list(self._history.get(rover_id, []))
            if n is None:
                return copy.deepcopy(hist)
            else:
                return copy.deepcopy(hist[-n:])

    async def list_rovers(self) -> List[str]:
        """Return list of rover_ids known to the store."""
        async with self._lock:
            return list(self._latest.keys())

    async def snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of all latest telemetry (shallow dict of deep-copied telemetry)."""
        async with self._lock:
            return {rid: copy.deepcopy(t) for rid, t in self._latest.items()}

    async def clear(self) -> None:
        """Clear stored telemetry (useful in tests)."""
        async with self._lock:
            self._latest.clear()
            self._history.clear()
            _logger.debug("TelemetryStore cleared")