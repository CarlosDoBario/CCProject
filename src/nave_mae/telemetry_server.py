"""
Telemetry TCP server for the Nave-Mãe.

- Accepts multiple concurrent TCP connections from rovers.
- Each connection sends JSON-lines (one JSON object per '\n').
- Recognised message types: "hello", "telemetry", "heartbeat".
- On "telemetry" the server updates the provided TelemetryStore and registers/updates the rover
  in the MissionStore (via mission_store.register_rover).
- Designed to integrate with existing repository layout:
    - TelemetryStore expected at nave_mae.telemetry_store (or any object with the same API)
    - MissionStore passed into the server so it can call register_rover(...)

Place this file at: src/nave_mae/telemetry_server.py
"""

import asyncio
import json
from typing import Optional, Dict, Any, Tuple
import logging

_logger = logging.getLogger("ml.telemetry_server")


def _safe_parse_json_line(line: bytes) -> Optional[Dict[str, Any]]:
    try:
        s = line.decode("utf-8").strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        _logger.exception("Failed to parse JSON line: %r", line[:200])
        return None


def _validate_telemetry_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    # Minimal validation rules
    rid = payload.get("rover_id") or payload.get("roverId") or payload.get("id")
    if not rid or not isinstance(rid, str):
        return False, "missing or invalid rover_id"
    pos = payload.get("position")
    if not pos or not isinstance(pos, dict):
        return False, "missing or invalid position"
    try:
        x = float(pos.get("x"))
        y = float(pos.get("y"))
    except Exception:
        return False, "position.x/position.y must be numeric"
    # state is recommended but allow it to be optional; still check if present it's a string
    state = payload.get("state")
    if state is not None and not isinstance(state, str):
        return False, "state must be a string"
    return True, ""


class TelemetryServer:
    """
    Asyncio-based TCP Telemetry server.

    Usage:
        server = TelemetryServer(mission_store=ms, telemetry_store=ts)
        await server.start(host="127.0.0.1", port=65080)
        ...
        await server.stop()
    """

    def __init__(self, mission_store, telemetry_store, host: str = "127.0.0.1", port: int = 65080):
        self._mission_store = mission_store
        self._telemetry_store = telemetry_store
        self.host = host
        self.port = port
        self._server: Optional[asyncio.base_events.Server] = None
        self._clients = set()  # track tasks for connections

    async def start(self) -> None:
        loop = asyncio.get_event_loop()
        self._server = await asyncio.start_server(self._handle_client, host=self.host, port=self.port)
        addr = self._server.sockets[0].getsockname() if self._server.sockets else (self.host, self.port)
        _logger.info("TelemetryServer listening on %s", addr)

    async def stop(self) -> None:
        _logger.info("Stopping TelemetryServer")
        # close server to stop accepting new connections
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        # cancel client handlers
        for t in list(self._clients):
            try:
                t.cancel()
            except Exception:
                pass
        # await for tasks to finish or be cancelled
        await asyncio.sleep(0)  # yield
        _logger.info("TelemetryServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        peer_addr = peer if isinstance(peer, tuple) else None
        _logger.info("Telemetry connection from %s", peer_addr)
        task = asyncio.current_task()
        if task:
            self._clients.add(task)
        try:
            # Optionally perform a short initial read to allow clients to send a "hello" first
            while True:
                line = await reader.readline()
                if line is None:
                    break
                if len(line) == 0:
                    # EOF - client closed connection
                    _logger.info("Telemetry client %s closed connection", peer_addr)
                    break
                payload = _safe_parse_json_line(line)
                if payload is None:
                    # parsing error already logged; continue reading
                    continue

                mtype = payload.get("type", "").lower()
                if mtype == "hello":
                    # optional hello payload: may include rover_id and metadata
                    rover_id = payload.get("rover_id") or payload.get("roverId") or payload.get("id")
                    if rover_id:
                        # update mission_store with rover registration (address)
                        try:
                            if peer_addr:
                                self._mission_store.register_rover(rover_id, address=peer_addr)
                            else:
                                self._mission_store.register_rover(rover_id)
                        except Exception:
                            _logger.exception("Failed to register rover %s from hello", rover_id)
                    # optionally send ack-like response
                    try:
                        resp = {"type": "hello_ack", "ts": payload.get("ts")}
                        writer.write((json.dumps(resp) + "\n").encode("utf-8"))
                        await writer.drain()
                    except Exception:
                        _logger.debug("Failed to send hello_ack to %s", peer_addr)
                    continue

                if mtype in ("heartbeat",):
                    rover_id = payload.get("rover_id") or payload.get("roverId") or payload.get("id")
                    if rover_id:
                        # update rover last_seen via mission_store.register_rover
                        try:
                            if peer_addr:
                                self._mission_store.register_rover(rover_id, address=peer_addr)
                            else:
                                self._mission_store.register_rover(rover_id)
                        except Exception:
                            _logger.exception("Failed to register rover from heartbeat %s", rover_id)
                    continue

                if mtype == "telemetry":
                    # Validate minimal telemetry and store it
                    ok, reason = _validate_telemetry_payload(payload)
                    if not ok:
                        _logger.warning("Invalid telemetry from %s: %s", peer_addr, reason)
                        # Optionally respond with error
                        try:
                            err = {"type": "error", "reason": reason}
                            writer.write((json.dumps(err) + "\n").encode("utf-8"))
                            await writer.drain()
                        except Exception:
                            pass
                        continue

                    # Determine rover_id and update mission_store registration
                    rover_id = payload.get("rover_id") or payload.get("roverId") or payload.get("id")
                    try:
                        if peer_addr:
                            self._mission_store.register_rover(rover_id, address=peer_addr)
                        else:
                            self._mission_store.register_rover(rover_id)
                    except Exception:
                        _logger.exception("Failed to register rover %s on telemetry receive", rover_id)

                    # Update TelemetryStore (await to preserve order)
                    try:
                        await self._telemetry_store.update(rover_id, payload)
                    except Exception:
                        _logger.exception("TelemetryStore.update failed for rover %s", rover_id)
                    # Optionally send an ack back to client
                    try:
                        ack = {"type": "ack", "ts": payload.get("ts")}
                        writer.write((json.dumps(ack) + "\n").encode("utf-8"))
                        await writer.drain()
                    except Exception:
                        # best-effort ack, ignore failures
                        pass
                    continue

                # Unknown message type: log and continue
                _logger.debug("Unknown telemetry message type from %s: %s", peer_addr, mtype)
        except asyncio.CancelledError:
            _logger.info("Telemetry handler cancelled for %s", peer_addr)
            raise
        except Exception:
            _logger.exception("Exception in telemetry client handler for %s", peer_addr)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if task:
                self._clients.discard(task)
            _logger.info("Telemetry connection handler finished for %s", peer_addr)