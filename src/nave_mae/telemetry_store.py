#!/usr/bin/env python3
"""
TelemetryStore for the Nave-Mãe.

Synchronous (thread-friendly) in-memory store that:
 - keeps the latest telemetry per rover,
 - retains a bounded history per rover,
 - exposes a simple synchronous API to update/query telemetry,
 - supports hooks to notify other components (e.g. MissionStore).
 - schedules async hooks (coroutines) on the asyncio loop when registered.

Rationale for sync API:
- The TS server code in this repo calls telemetry_store.update(...) from an asyncio
  handler and (in the current integration) does not await it. To keep the integration
  simple we implement update() as a synchronous call protected by a threading.Lock,
  while still supporting async hook functions (they are scheduled via asyncio).
"""

from typing import Any, Dict, List, Callable, Optional, Tuple
import threading
import inspect
import copy
from collections import deque, defaultdict
from datetime import datetime, timezone
import asyncio

from common import utils, binary_proto

logger = utils.get_logger("nave_mae.telemetry_store")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ms_to_iso(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return _now_iso()


class TelemetryStore:
    """
    TelemetryStore

    Public API (synchronous):
      - register_hook(fn)
      - update(rover_id: str, telemetry: Dict[str,Any], addr: Optional[Tuple[str,int]] = None) -> None
      - get_latest(rover_id: str) -> Optional[Dict[str,Any]]
      - get_history(rover_id: str, n: Optional[int] = None) -> List[Dict[str,Any]]
      - list_rovers() -> List[str]
      - snapshot() -> Dict[str,Any]
      - clear() -> None

    Hooks: fn(event_type: str, payload: dict) can be either sync function or async coroutine function.
    Async hooks are scheduled on the running asyncio event loop (if present) using create_task.
    """

    def __init__(self, history_size: int = 100):
        # latest telemetry per rover_id
        self._latest: Dict[str, Dict[str, Any]] = {}
        # history per rover_id (deque)
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
        # threading lock to allow sync update() to be called from any thread / coroutine
        self._lock = threading.Lock()
        # hooks to call on new telemetry: fn(event_type: str, payload: dict)
        self._hooks: List[Callable[[str, Dict[str, Any]], Any]] = []

    def register_hook(self, fn: Callable[[str, Dict[str, Any]], Any]) -> None:
        """
        Register a hook to be called on new telemetry updates.
        Hook signature: fn(event_type: str, payload: dict)
        Hook may be synchronous or an async coroutine function.
        """
        self._hooks.append(fn)
        logger.debug("Registered telemetry hook %s", getattr(fn, "__name__", repr(fn)))

    def _schedule_hook_call(self, fn: Callable[[str, Dict[str, Any]], Any], event_type: str, payload: Dict[str, Any]) -> None:
        """
        Schedule or call a single hook. If fn is a coroutine function, schedule it on the
        running asyncio loop (best-effort). Otherwise call synchronously.
        """
        try:
            if inspect.iscoroutinefunction(fn):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop:
                    loop.create_task(fn(event_type, payload))
                else:
                    # No running loop in this thread: spawn a background task to run the coroutine
                    # in a new loop to avoid losing the hook (best-effort).
                    def _run_coro_in_new_loop(coro_fn, ev, pl):
                        try:
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            new_loop.run_until_complete(coro_fn(ev, pl))
                        except Exception:
                            logger.exception("hook (background) failed")
                        finally:
                            try:
                                new_loop.close()
                            except Exception:
                                pass
                    threading.Thread(target=_run_coro_in_new_loop, args=(fn, event_type, payload), daemon=True).start()
            else:
                # sync call
                try:
                    fn(event_type, payload)
                except Exception:
                    logger.exception("telemetry hook (sync) error")
        except Exception:
            logger.exception("telemetry hook scheduling error")

    def _call_hooks(self, event_type: str, payload: Dict[str, Any]) -> None:
        """
        Call all registered hooks (synchronously schedule async hooks).
        Designed to be called outside of heavy locks (we call it after updating store).
        """
        for fn in list(self._hooks):
            self._schedule_hook_call(fn, event_type, payload)

    def update(self, rover_id: str, telemetry: Dict[str, Any], addr: Optional[Tuple[str, int]] = None) -> None:
        """
        Record a telemetry sample for rover_id.

        telemetry: canonical dict as produced by binary_proto.tlv_to_canonical() or similar.
        addr: optional peer address tuple (ip, port) provided by the server.

        This method is synchronous and protected by a threading.Lock so it can be called
        directly from asyncio handlers without awaiting.
        """
        if not isinstance(telemetry, dict):
            raise ValueError("telemetry must be a dict")

        # make shallow copy and normalize timestamps/fields
        tel = dict(telemetry)

        # normalize timestamp: prefer server-provided _ts_server_received_ms, then known keys
        ts_ms = None
        if tel.get("_ts_server_received_ms") is not None:
            try:
                ts_ms = int(tel.get("_ts_server_received_ms"))
            except Exception:
                ts_ms = None
        elif tel.get("ts_ms") is not None:
            try:
                ts_ms = int(tel.get("ts_ms"))
            except Exception:
                ts_ms = None
        elif tel.get("timestamp") is not None:
            # ISO string provided: convert
            try:
                ts_ms = utils.iso_to_epoch_ms(tel.get("timestamp"))
            except Exception:
                ts_ms = None

        if ts_ms is None:
            ts_ms = utils.now_epoch_ms()

        tel["ts_ms"] = int(ts_ms)
        tel["ts"] = _ms_to_iso(int(ts_ms))

        # attach rover_id and address for convenience
        tel["rover_id"] = rover_id
        if addr:
            tel["_addr"] = (addr[0], int(addr[1]) if addr[1] is not None else None)

        with self._lock:
            # store latest (deep copy)
            self._latest[rover_id] = copy.deepcopy(tel)
            # append to history
            self._history[rover_id].append(copy.deepcopy(tel))
            logger.debug("Telemetry updated for %s ts=%s keys=%s", rover_id, tel.get("ts"), list(tel.keys()))

        # notify hooks (outside lock)
        try:
            self._call_hooks("telemetry", {"rover_id": rover_id, "telemetry": copy.deepcopy(tel)})
        except Exception:
            logger.exception("Error calling telemetry hooks")

    def get_latest(self, rover_id: str) -> Optional[Dict[str, Any]]:
        """Return a deep copy of latest telemetry for rover_id or None."""
        with self._lock:
            t = self._latest.get(rover_id)
            return copy.deepcopy(t) if t is not None else None

    def get_history(self, rover_id: str, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Return up to n most recent telemetry samples for rover_id.
        If n is None, return the entire retained history (bounded by history_size).
        Results are ordered oldest->newest.
        """
        with self._lock:
            hist = list(self._history.get(rover_id, []))
            if n is None:
                return copy.deepcopy(hist)
            else:
                return copy.deepcopy(hist[-n:])

    def list_rovers(self) -> List[str]:
        """Return list of rover_ids known to the store."""
        with self._lock:
            return list(self._latest.keys())

    def snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of all latest telemetry (shallow dict of deep-copied telemetry)."""
        with self._lock:
            return {rid: copy.deepcopy(t) for rid, t in self._latest.items()}

    def clear(self) -> None:
        """Clear stored telemetry (useful in tests)."""
        with self._lock:
            self._latest.clear()
            self._history.clear()
            logger.debug("TelemetryStore cleared")