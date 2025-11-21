#!/usr/bin/env python3
"""
telemetry_persister.py

Telemetry persister worker.

Provides a lightweight background worker that can be attached to a TelemetryStore
(and other hook-based emitters) to persist canonical telemetry records as NDJSON
lines into daily files under a given directory.

Features:
- Non-blocking hook: the hook enqueues records to an internal queue and returns
  immediately. If queue is full, records are dropped (with a debug log).
- Background writer thread flushes queue to disk, rotates files daily (by UTC date)
  and ensures atomic append writes.
- start() / stop() API to control lifecycle; safe to call from main thread or test code.
- attach_to_store(store) helper to register the persister as a hook on a TelemetryStore.

Intended usage (example):
    persister = TelemetryPersister(outdir="/var/log/telemetry", prefix="telemetry")
    persister.start()
    persister.attach_to_store(telemetry_store)
    ...
    persister.stop()

Notes:
- The worker writes NDJSON lines with a small buffer; in tests it's acceptable to
  wait a short period after sending telemetry so the persister can flush.
"""
from __future__ import annotations

import json
import os
import threading
import queue
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable

from common import utils

logger = utils.get_logger("nave_mae.telemetry_persister")


def _ymd_from_ts_ms(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y%m%d")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y%m%d")


class TelemetryPersister:
    """
    Persist telemetry canonical records as NDJSON into daily files.

    Parameters:
    - outdir: directory where per-day files will be written
    - prefix: filename prefix, final name: "{prefix}-{YYYYMMDD}.ndjson"
    - queue_maxsize: maximum items queued before dropping
    - flush_interval_s: how often background thread flushes (seconds)
    """

    def __init__(self, outdir: str, prefix: str = "telemetry", queue_maxsize: int = 10000, flush_interval_s: float = 0.5):
        self.outdir = os.path.abspath(outdir)
        self.prefix = prefix
        self.queue_maxsize = int(queue_maxsize)
        self.flush_interval_s = float(flush_interval_s)

        self._q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=self.queue_maxsize)
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._started = False
        self._file_handle = None
        self._current_ymd: Optional[str] = None
        self._fh_lock = threading.Lock()

        # ensure outdir exists
        os.makedirs(self.outdir, exist_ok=True)

    # Hook function to register on TelemetryStore (synchronous)
    def hook(self, event_type: str, payload: Dict[str, Any]) -> None:
        """
        Hook compatible with TelemetryStore.register_hook(fn).
        Enqueues a small dict containing essential record (timestamp, rover_id, telemetry).
        The payload structure used by TelemetryStore is: {"rover_id": ..., "telemetry": {...}}
        We persist the telemetry dict augmented with rover_id and ts_ms.
        """
        try:
            # Normalize expected shapes
            rec = None
            if isinstance(payload, dict) and "telemetry" in payload:
                tel = payload.get("telemetry") or {}
                if isinstance(tel, dict):
                    rec = dict(tel)
                else:
                    # fallback: store payload as payload_json
                    rec = {"payload_json": tel}
                # ensure rover_id is present
                if "rover_id" in payload and payload["rover_id"]:
                    rec["rover_id"] = payload["rover_id"]
            elif isinstance(payload, dict) and "rover_id" in payload and "telemetry" not in payload:
                rec = dict(payload)
            else:
                # unknown shape: wrap
                rec = {"rover_id": payload.get("rover_id") if isinstance(payload, dict) else None, "payload_json": payload}

            # ensure ts_ms present
            ts_ms = rec.get("ts_ms") or rec.get("_ts_server_received_ms") or rec.get("timestamp_ms")
            try:
                if ts_ms is not None:
                    rec["ts_ms"] = int(ts_ms)
                else:
                    rec["ts_ms"] = int(time.time() * 1000)
            except Exception:
                rec["ts_ms"] = int(time.time() * 1000)

            # enqueue non-blocking
            try:
                self._q.put_nowait(rec)
            except queue.Full:
                # drop record to avoid blocking critical server loop. Log at debug to avoid spam.
                logger.debug("TelemetryPersister queue full; dropping telemetry for rover=%s ts=%s", rec.get("rover_id"), rec.get("ts_ms"))
        except Exception:
            logger.exception("TelemetryPersister.hook failed while enqueuing payload")

    def _open_file_for_ymd(self, ymd: str):
        """
        Open file handle for given ymd, rotating if needed.
        """
        fname = os.path.join(self.outdir, f"{self.prefix}-{ymd}.ndjson")
        # open in append mode, UTF-8
        fh = open(fname, "a", encoding="utf-8")
        return fh

    def _rotate_if_needed(self, ts_ms: int):
        ymd = _ymd_from_ts_ms(ts_ms)
        if self._current_ymd != ymd or self._file_handle is None:
            # rotate
            with self._fh_lock:
                try:
                    if self._file_handle:
                        try:
                            self._file_handle.flush()
                        except Exception:
                            pass
                        try:
                            self._file_handle.close()
                        except Exception:
                            pass
                    self._file_handle = self._open_file_for_ymd(ymd)
                    self._current_ymd = ymd
                except Exception:
                    logger.exception("TelemetryPersister: failed rotating file for date=%s", ymd)
                    # ensure handle is None to avoid writes
                    self._file_handle = None
                    self._current_ymd = None

    def _writer_loop(self):
        logger.info("TelemetryPersister: writer thread starting; outdir=%s", self.outdir)
        last_flush = time.time()
        try:
            while not self._stop_evt.is_set():
                try:
                    rec = self._q.get(timeout=self.flush_interval_s)
                except queue.Empty:
                    # periodic flush
                    rec = None
                if rec is not None:
                    try:
                        ts_ms = int(rec.get("ts_ms", int(time.time() * 1000)))
                    except Exception:
                        ts_ms = int(time.time() * 1000)
                    # rotate file if date changed
                    self._rotate_if_needed(ts_ms)
                    line = None
                    try:
                        line = json.dumps(rec, ensure_ascii=False)
                    except Exception:
                        try:
                            # fallback: coerce to str
                            line = json.dumps({"rover_id": rec.get("rover_id"), "ts_ms": rec.get("ts_ms"), "payload": str(rec.get("payload_json", ""))}, ensure_ascii=False)
                        except Exception:
                            line = None
                    if line is not None and self._file_handle:
                        try:
                            with self._fh_lock:
                                self._file_handle.write(line + "\n")
                        except Exception:
                            logger.exception("TelemetryPersister: failed writing line")
                    # allow GC of rec
                    try:
                        self._q.task_done()
                    except Exception:
                        pass

                # flush periodically
                now = time.time()
                if now - last_flush >= self.flush_interval_s:
                    last_flush = now
                    if self._file_handle:
                        try:
                            with self._fh_lock:
                                self._file_handle.flush()
                        except Exception:
                            pass

            # drain queue on stop
            while True:
                try:
                    rec = self._q.get_nowait()
                except queue.Empty:
                    break
                try:
                    ts_ms = int(rec.get("ts_ms", int(time.time() * 1000)))
                except Exception:
                    ts_ms = int(time.time() * 1000)
                self._rotate_if_needed(ts_ms)
                try:
                    line = json.dumps(rec, ensure_ascii=False)
                except Exception:
                    try:
                        line = json.dumps({"rover_id": rec.get("rover_id"), "ts_ms": rec.get("ts_ms"), "payload": str(rec.get("payload_json", ""))}, ensure_ascii=False)
                    except Exception:
                        line = None
                if line and self._file_handle:
                    try:
                        with self._fh_lock:
                            self._file_handle.write(line + "\n")
                    except Exception:
                        logger.exception("TelemetryPersister: failed writing during drain")
                try:
                    self._q.task_done()
                except Exception:
                    pass

            # final flush / close
            if self._file_handle:
                try:
                    with self._fh_lock:
                        self._file_handle.flush()
                        self._file_handle.close()
                except Exception:
                    pass
                self._file_handle = None
                self._current_ymd = None

        except Exception:
            logger.exception("TelemetryPersister: writer thread crashed unexpectedly")
        finally:
            logger.info("TelemetryPersister: writer thread exiting")

    def start(self) -> None:
        """
        Start the background writer thread. Safe to call multiple times (idempotent).
        """
        if self._started:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._writer_loop, name="telemetry-persister", daemon=True)
        self._thread.start()
        self._started = True
        logger.info("TelemetryPersister started (outdir=%s)", self.outdir)

    def stop(self, wait: bool = True, timeout: float = 2.0) -> None:
        """
        Stop the background writer thread and flush queued items.

        Parameters:
        - wait: if True, join the thread (blocks up to `timeout` seconds)
        - timeout: max seconds to wait for join
        """
        if not self._started:
            return
        self._stop_evt.set()
        if wait and self._thread:
            try:
                self._thread.join(timeout=timeout)
            except Exception:
                pass
        # best-effort: if thread still alive, log a warning
        if self._thread and self._thread.is_alive():
            logger.warning("TelemetryPersister: writer thread did not stop within timeout")

        # reset started state
        self._started = False

    def attach_to_store(self, telemetry_store) -> Callable[[], None]:
        """
        Register this persister as a hook on telemetry_store and return an "undo" callable
        that can be used to unregister the hook.

        The TelemetryStore.register_hook API is expected to accept a synchronous callable
        of signature fn(event_type: str, payload: dict).
        """
        try:
            telemetry_store.register_hook(self.hook)
        except Exception:
            # fallbacks for alternative hook APIs
            try:
                telemetry_store.register_handler(self.hook)
            except Exception:
                try:
                    telemetry_store.add_listener("telemetry", self.hook)
                except Exception:
                    logger.exception("TelemetryPersister: failed to attach hook to telemetry_store")
                    raise

        def _detach():
            try:
                # best-effort removal depending on API
                if hasattr(telemetry_store, "unregister_hook"):
                    try:
                        telemetry_store.unregister_hook(self.hook)
                        return
                    except Exception:
                        pass
                if hasattr(telemetry_store, "remove_listener"):
                    try:
                        telemetry_store.remove_listener("telemetry", self.hook)
                        return
                    except Exception:
                        pass
                # last resort: try to remove from internal hooks list if it exists
                if hasattr(telemetry_store, "_hooks") and isinstance(getattr(telemetry_store, "_hooks"), list):
                    try:
                        telemetry_store._hooks.remove(self.hook)
                    except Exception:
                        pass
            except Exception:
                logger.exception("TelemetryPersister: detach failed")

        return _detach