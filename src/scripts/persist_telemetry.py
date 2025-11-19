#!/usr/bin/env python3
"""
persist_telemetry.py

Exemplo simples que arranca um TelemetryServer in-process (standalone) e persiste
todas as amostras de telemetria recebidas em ficheiros NDJSON (one JSON object per line).

Objetivo: demonstrar persistência sem tocar no protocolo binário (TS framing / TLVs).
Uso:
  PowerShell:
    $env:PYTHONPATH = "src"; python src/scripts/persist_telemetry.py
  Bash:
    PYTHONPATH=src python src/scripts/persist_telemetry.py

O script cria o diretório <DATA_DIR>/telemetry e escreve ficheiros do tipo:
  telemetry-YYYYMMDD.ndjson
"""
from __future__ import annotations

import threading
import queue
import time
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import argparse
import logging
import signal
import sys
import asyncio
from typing import Any, Dict

from common import config, utils
from nave_mae.telemetry_launcher import start_telemetry_server

logger = utils.get_logger("scripts.persist_telemetry")


def _now_ymd(ts: float | None = None) -> str:
    t = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
    return t.strftime("%Y%m%d")


class NDJSONWriter:
    """
    Simple NDJSON writer with daily rotation and background thread.
    Accepts Python dicts via .put(record) and writes each as a line JSON to daily file.
    """
    def __init__(self, out_dir: Path, filename_prefix: str = "telemetry"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.filename_prefix = filename_prefix
        self._queue: "queue.Queue[Dict[str,Any]]" = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._stop_evt = threading.Event()
        self._current_date = None
        self._fh = None

    def start(self):
        logger.info("Starting NDJSONWriter; files in %s", str(self.out_dir))
        self._thread.start()

    def stop(self):
        logger.info("Stopping NDJSONWriter")
        self._stop_evt.set()
        # wake worker
        self._queue.put(None)  # sentinel
        self._thread.join(timeout=5.0)
        try:
            if self._fh:
                self._fh.flush()
                self._fh.close()
                self._fh = None
        except Exception:
            pass

    def put(self, record: Dict[str, Any]):
        # best-effort: don't block forever
        try:
            self._queue.put(record, block=False)
        except queue.Full:
            logger.warning("Persist queue full: dropping telemetry record")

    def _open_for_date(self, ymd: str):
        if self._fh:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass
            self._fh = None
        fname = f"{self.filename_prefix}-{ymd}.ndjson"
        path = self.out_dir / fname
        # open in append mode, create if not exists
        self._fh = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered
        self._current_date = ymd
        logger.info("Opened telemetry file %s", str(path))

    def _worker(self):
        """
        Worker consumes queue and writes NDJSON lines. Rotates daily.
        """
        try:
            while not self._stop_evt.is_set():
                try:
                    item = self._queue.get(timeout=1.0)
                except queue.Empty:
                    # on idle, flush file occasionally
                    if self._fh:
                        try:
                            self._fh.flush()
                        except Exception:
                            pass
                    continue
                if item is None:
                    # sentinel to stop
                    break
                # compute target date
                ts_ms = item.get("ts_ms") or item.get("_ts_server_received_ms") or item.get("timestamp_ms") or int(time.time() * 1000)
                ymd = _now_ymd(ts_ms / 1000.0)
                if self._current_date != ymd or self._fh is None:
                    self._open_for_date(ymd)
                try:
                    # ensure JSON serializable (payload_json may already be a dict)
                    line = json.dumps(item, ensure_ascii=False)
                    self._fh.write(line + "\n")
                except Exception:
                    logger.exception("Failed to write telemetry record")
        except Exception:
            logger.exception("NDJSONWriter worker crashed")
        finally:
            try:
                if self._fh:
                    self._fh.flush()
                    self._fh.close()
                    self._fh = None
            except Exception:
                pass
            logger.info("NDJSONWriter worker exiting")


def telemetry_hook_factory(writer: NDJSONWriter):
    """
    Return a hook function suitable to register with TelemetryStore.register_hook(fn)
    Signature expected: fn(event_type: str, payload: dict)
    The TelemetryStore in this project calls hooks with {"rover_id":..., "telemetry": {...}}
    We'll normalize and persist a compact record.
    """
    def hook(event_type: str, payload: Dict[str, Any]):
        try:
            # payload may be {"rover_id": rid, "telemetry": {...}} or a dict already
            if isinstance(payload, dict) and "telemetry" in payload and isinstance(payload["telemetry"], dict):
                rid = payload.get("rover_id") or payload["telemetry"].get("rover_id")
                tel = dict(payload["telemetry"])
            elif isinstance(payload, dict) and "rover_id" in payload and "telemetry" not in payload:
                # sometimes hook gets {"rover_id": rid, "telemetry": {...}} but fallback
                rid = payload.get("rover_id")
                tel = dict(payload)
            else:
                # unknown shape; attempt to record as-is
                rid = payload.get("rover_id") if isinstance(payload, dict) else None
                tel = payload if isinstance(payload, dict) else {"value": str(payload)}

            # canonicalize record fields we care about
            rec = {
                "ts_ms": int(tel.get("ts_ms") or tel.get("timestamp_ms") or int(time.time() * 1000)),
                "rover_id": rid or tel.get("rover_id") or "<unknown>",
                "mission_id": tel.get("mission_id"),
                "position": tel.get("position"),
                "progress_pct": tel.get("progress_pct"),
                "battery_level_pct": tel.get("battery_level_pct"),
                "status": tel.get("status"),
                # keep full payload for flexibility
                "payload_json": tel,
                # metadata
                "_addr": tel.get("_addr") or payload.get("_addr") or None,
                "_msgid": tel.get("_msgid") or None,
                "_ts_server_received_ms": tel.get("_ts_server_received_ms") or None,
            }
            writer.put(rec)
        except Exception:
            logger.exception("telemetry_hook: failed to process payload")
    return hook


async def main_async(host: str = None, port: int = None, outdir: str = None):
    host = host or config.TELEMETRY_HOST
    port = port or config.TELEMETRY_PORT
    data_dir = Path(config.DATA_DIR)
    outdir = outdir or str(data_dir / "telemetry")
    writer = NDJSONWriter(Path(outdir))
    writer.start()

    # start telemetry server (creates TelemetryStore if needed)
    services = await start_telemetry_server(mission_store=None, telemetry_store=None, host=host, port=port, persist_file=None)
    ms = services.get("ms")
    ts = services.get("ts")
    server = services.get("telemetry_server")
    logger.info("Started TelemetryServer for persistence demo on %s:%s", host, port)

    # register hook on telemetry_store when available
    hook = telemetry_hook_factory(writer)
    attached = False
    try:
        if ts and hasattr(ts, "register_hook"):
            ts.register_hook(hook)
            attached = True
            logger.info("Attached persistence hook to TelemetryStore")
    except Exception:
        logger.exception("Failed to attach hook to TelemetryStore")

    # fallback: try attach to server object if it exposes register_hook-like
    if not attached:
        try:
            if server and hasattr(server, "register_hook"):
                server.register_hook(hook)
                attached = True
                logger.info("Attached persistence hook to TelemetryServer")
        except Exception:
            logger.exception("Failed to attach hook to TelemetryServer")

    # Run until signal (Ctrl-C)
    loop = asyncio.get_running_loop()
    stop_evt = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Stop requested")
        stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except Exception:
            # Windows may not support add_signal_handler for all signals; ignore
            pass

    await stop_evt.wait()

    # cleanup
    try:
        if server:
            await server.stop()
    except Exception:
        logger.exception("Error stopping server")
    writer.stop()
    logger.info("Persistence script exiting")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=config.TELEMETRY_HOST)
    parser.add_argument("--port", type=int, default=config.TELEMETRY_PORT)
    parser.add_argument("--outdir", default=None, help="Directory to store telemetry NDJSON files (default: DATA_DIR/telemetry)")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(host=args.host, port=args.port, outdir=args.outdir))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception:
        logger.exception("persist_telemetry main crashed")


if __name__ == "__main__":
    main()