#!/usr/bin/env python3
"""
TelemetryStore for the Nave-Mãe.

Synchronous (thread-friendly) in-memory store that:
 - keeps the latest telemetry per rover,
 - retains a bounded history per rover,
 - exposes a simple synchronous API to update/query telemetry,
 - supports hooks to notify other components (e.g. MissionStore).
 - schedules async hooks (coroutines) on a dedicated background loop when no running
   asyncio loop is available in the caller thread.

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
import time

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
      - close() -> None  # graceful shutdown for background hook loop

    Hooks: fn(event_type: str, payload: dict) can be either sync function or async coroutine function.
    Async hooks are scheduled on the running asyncio event loop (if present) using create_task.
    If no running loop is available, a dedicated background asyncio loop (running in a daemon
    thread) is started and coroutines are submitted to it via run_coroutine_threadsafe.

    This avoids starting a new event loop per hook call (previous behaviour) and makes scheduling
    reliable and efficient across threads.
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

        # Background hook loop for scheduling coroutine hooks when no running loop available
        self._hook_loop: Optional[asyncio.AbstractEventLoop] = None
        self._hook_loop_thread: Optional[threading.Thread] = None
        self._hook_loop_lock = threading.Lock()
        self._hook_loop_ready = threading.Event()
        self._closed = False

    def register_hook(self, fn: Callable[[str, Dict[str, Any]], Any]) -> None:
        """
        Register a hook to be called on new telemetry updates.
        Hook signature: fn(event_type: str, payload: dict)
        Hook may be synchronous or an async coroutine function.
        """
        self._hooks.append(fn)
        logger.debug("Registered telemetry hook %s", getattr(fn, "__name__", repr(fn)))

    def _start_background_hook_loop(self) -> None:
        """
        Start a background asyncio event loop in a daemon thread for scheduling coroutine hooks.
        This is started lazily on demand (first time we need to schedule a coroutine hook
        and there's no running event loop available).
        """
        with self._hook_loop_lock:
            if self._hook_loop_thread is not None and self._hook_loop is not None and self._hook_loop.is_running():
                return
            # create and start loop in thread
            def _run_loop():
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    self._hook_loop = loop
                    self._hook_loop_ready.set()
                    loop.run_forever()
                    # graceful shutdown
                    pending = asyncio.all_tasks(loop=loop)
                    if pending:
                        for t in pending:
                            t.cancel()
                        try:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                        except Exception:
                            pass
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    logger.exception("Background hook loop crashed")
                finally:
                    try:
                        self._hook_loop_ready.clear()
                    except Exception:
                        pass
                    self._hook_loop = None

            th = threading.Thread(target=_run_loop, name="telemetry-hooks-loop", daemon=True)
            self._hook_loop_thread = th
            th.start()
            # wait briefly until loop is ready
            if not self._hook_loop_ready.wait(timeout=1.0):
                logger.warning("Background hook loop did not start within timeout")

    def _stop_background_hook_loop(self) -> None:
        with self._hook_loop_lock:
            if self._hook_loop is None or self._hook_loop_thread is None:
                return
            try:
                loop = self._hook_loop
                # stop loop
                loop.call_soon_threadsafe(loop.stop)
            except Exception:
                logger.exception("Failed to stop background hook loop")
            # join thread with timeout
            th = self._hook_loop_thread
            self._hook_loop_thread = None
            try:
                th.join(timeout=1.0)
            except Exception:
                pass
            self._hook_loop = None
            self._hook_loop_ready.clear()

    def _schedule_hook_call(self, fn: Callable[[str, Dict[str, Any]], Any], event_type: str, payload: Dict[str, Any]) -> None:
        """
        Schedule or call a single hook. If fn is a coroutine function, schedule it on the
        running asyncio loop (best-effort). Otherwise call synchronously.

        Improvements over previous behavior:
         - If there is a running loop in the current thread, schedule with create_task.
         - Else schedule on a shared background loop (started lazily) via run_coroutine_threadsafe.
         - Avoids creating a new event loop per hook invocation.
        """
        try:
            if inspect.iscoroutinefunction(fn):
                try:
                    # Prefer using the current running loop if available
                    loop = None
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop:
                        # schedule on current running loop
                        loop.create_task(fn(event_type, payload))
                    else:
                        # ensure background loop is running
                        self._start_background_hook_loop()
                        if self._hook_loop:
                            try:
                                asyncio.run_coroutine_threadsafe(fn(event_type, payload), self._hook_loop)
                            except Exception:
                                logger.exception("hook (background) scheduling failed")
                        else:
                            # last-resort: run coroutine synchronously in a new loop (rare)
                            try:
                                new_loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(new_loop)
                                new_loop.run_until_complete(fn(event_type, payload))
                                try:
                                    new_loop.close()
                                except Exception:
                                    pass
                            except Exception:
                                logger.exception("hook (fallback sync) failed")
                except Exception:
                    logger.exception("hook (coroutine) scheduling error")
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

    def close(self) -> None:
        """
        Gracefully stop any background resources used by the store (e.g. background hook loop).
        Should be called when TelemetryStore is no longer needed (tests/cleanup).
        """
        if self._closed:
            return
        self._closed = True
        try:
            # stop background hook loop if started
            self._stop_background_hook_loop()
        except Exception:
            logger.exception("Error closing TelemetryStore background hook loop")

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass