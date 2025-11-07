#!/usr/bin/env python3
"""
ml_server.py
Implementação MissionLink (ML) UDP server (Nave-Mãe)

Atualizado: PendingOutgoing guarda message_type/mission_id; adicionada lógica para envio de
MISSION_CANCEL e processamento de ACKs de cancelamento para marcar missão como CANCELLED.
"""

import asyncio
import time
import contextlib
from typing import Dict, Any, Optional, Tuple
import traceback

from common import ml_schema, config, utils

logger = utils.get_logger("ml.server")

# Importar mission_store se disponível; else fornecer um minimal fallback.
try:
    from nave_mae.mission_store import MissionStore  # type: ignore
except Exception:
    logger.warning("nave_mae.mission_store not found — using minimal in-memory fallback")

    class MissionStore:
        """
        Minimal fallback mission store for development.
        """
        def __init__(self):
            self.missions: Dict[str, Dict[str, Any]] = {}
            self.rovers: Dict[str, Dict[str, Any]] = {}
            self._next_mission = 1

        def register_rover(self, rover_id: str, address: Tuple[str, int]):
            self.rovers[rover_id] = {
                "rover_id": rover_id,
                "address": address,
                "state": "IDLE",
                "last_seen": utils.now_iso(),
            }

        def create_mission(self, mission_spec: Dict[str, Any]) -> str:
            mid = f"M-{self._next_mission:03d}"
            self._next_mission += 1
            m = mission_spec.copy()
            m.update({"mission_id": mid, "state": "CREATED", "assigned_rover": None, "history": []})
            self.missions[mid] = m
            return mid

        def get_pending_mission_for_rover(self, rover_id: str) -> Optional[Dict[str, Any]]:
            for m in self.missions.values():
                if m.get("assigned_rover") is None:
                    return m
            return None

        def assign_mission_to_rover(self, mission: Dict[str, Any], rover_id: str):
            mid = mission["mission_id"]
            mission["assigned_rover"] = rover_id
            mission["state"] = "ASSIGNED"
            mission["assigned_at"] = utils.now_iso()
            self.rovers.setdefault(rover_id, {})["state"] = "ASSIGNED"

        def update_progress(self, mission_id: str, rover_id: str, progress_payload: Dict[str, Any]):
            m = self.missions.get(mission_id)
            if m:
                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "PROGRESS", "payload": progress_payload})
                m["state"] = "IN_PROGRESS"
                self.rovers.setdefault(rover_id, {})["state"] = "IN_MISSION"

        def complete_mission(self, mission_id: str, rover_id: str, result_payload: Dict[str, Any]):
            m = self.missions.get(mission_id)
            if m:
                m["state"] = "COMPLETED"
                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "COMPLETE", "payload": result_payload})
                self.rovers.setdefault(rover_id, {})["state"] = "IDLE"

        def list_missions(self):
            return list(self.missions.values())

        def get_mission(self, mission_id: str):
            return self.missions.get(mission_id)

        def unassign_mission(self, mission_id: str, reason: Optional[str] = None):
            m = self.missions.get(mission_id)
            if not m:
                return
            prev = m.get("assigned_rover")
            m["assigned_rover"] = None
            m["state"] = "CREATED"
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "ASSIGN_FAILED", "reason": reason})
            if prev:
                self.rovers.setdefault(prev, {})["state"] = "IDLE"

        def cancel_mission(self, mission_id: str, reason: Optional[str] = None):
            m = self.missions.get(mission_id)
            if not m:
                return
            prev = m.get("assigned_rover")
            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL", "reason": reason})
            m["state"] = "CANCELLED"
            m["cancelled_at"] = utils.now_iso()
            if prev:
                self.rovers.setdefault(prev, {})["state"] = "IDLE"

        def get_rover(self, rover_id: str):
            return self.rovers.get(rover_id)


class PendingOutgoing:
    """
    Representa uma mensagem enviada pelo servidor que aguarda ACK.

    Guarda message_type e mission_id (quando disponíveis) para permitir
    decisão de política quando retries esgotam.
    """
    def __init__(self, msg_id: str, packet: bytes, addr: Tuple[str, int], created_at: float, timeout_s: float,
                 message_type: Optional[str] = None, mission_id: Optional[str] = None):
        self.msg_id = msg_id
        self.packet = packet
        self.addr = addr
        self.created_at = created_at
        self.next_timeout = created_at + timeout_s
        self.attempts = 0
        self.timeout_s = timeout_s
        # additional metadata
        self.message_type = message_type
        self.mission_id = mission_id


class MLServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, mission_store: MissionStore):
        super().__init__()
        self.transport = None
        self.mission_store = mission_store

        # Dedup cache: msg_id -> timestamp
        self.seen_msgs: Dict[str, float] = {}

        # Pending outgoing messages that require ACK: msg_id -> PendingOutgoing
        self.pending_outgoing: Dict[str, PendingOutgoing] = {}

        # Store last ACKs sent for duplicated handling (msg_id -> ack_packet)
        self.last_acks: Dict[str, bytes] = {}

        # Metrics
        self.metrics = utils.metrics

        # Async tasks
        self._task_retransmit = None
        self._task_cleanup = None

        # RTT estimation (simple)
        self.rtt_estimate = None

    def connection_made(self, transport):
        self.transport = transport
        sockname = transport.get_extra_info("sockname")
        logger.info(f"ML server listening on {sockname}")
        # Start background tasks
        loop = asyncio.get_event_loop()
        self._task_retransmit = loop.create_task(self._retransmit_loop())
        self._task_cleanup = loop.create_task(self._cleanup_loop())

    def connection_lost(self, exc):
        logger.info("ML server connection lost")
        if self._task_retransmit:
            self._task_retransmit.cancel()
        if self._task_cleanup:
            self._task_cleanup.cancel()

    def datagram_received(self, data: bytes, addr):
        # Entry point for incoming datagrams
        try:
            envelope = ml_schema.parse_envelope(data)
        except Exception as e:
            logger.warning(f"Failed to parse envelope from {addr}: {e}")
            # Optionally send ERROR back (can't rely on parse to get msg_id)
            return

        header = envelope["header"]
        body = envelope["body"]
        msg_id = header["msg_id"]
        mtype = header["message_type"]
        rover_id = header.get("rover_id")
        logger.debug(f"Received {mtype} msg_id={msg_id} from {addr} rover_id={rover_id}")

        # Dedup check
        if msg_id in self.seen_msgs:
            logger.debug(f"Duplicate msg {msg_id} from {addr} — resending ACK if known")
            # If we have previously sent an ACK for this msg, re-send it
            ack_packet = self.last_acks.get(msg_id)
            if ack_packet:
                self.transport.sendto(ack_packet, addr)
            return
        # mark seen
        self.seen_msgs[msg_id] = time.time()

        # Register rover if rover_id present
        if rover_id:
            try:
                self.mission_store.register_rover(rover_id, addr)
            except Exception:
                logger.exception("mission_store.register_rover failed (continuing)")

        # Dispatch by message_type
        try:
            if mtype == "REQUEST_MISSION":
                self._handle_request_mission(envelope, addr)
            elif mtype == "PROGRESS":
                self._handle_progress(envelope, addr)
                # send ACK
                ack_env = ml_schema.make_ack(msg_id, rover_id=rover_id, mission_id=header.get("mission_id"))
                self._send_ack_immediate(ack_env, addr)
            elif mtype == "MISSION_COMPLETE":
                self._handle_complete(envelope, addr)
                ack_env = ml_schema.make_ack(msg_id, rover_id=rover_id, mission_id=header.get("mission_id"))
                self._send_ack_immediate(ack_env, addr)
            elif mtype == "ACK":
                # ACK for message we sent
                acked = body.get("acked_msg_id")
                if acked:
                    self._handle_incoming_ack(acked, addr)
            elif mtype == "ERROR":
                logger.warning(f"Received ERROR from {rover_id}@{addr}: {body}")
                # react if needed
            elif mtype == "HEARTBEAT":
                ack_env = ml_schema.make_ack(msg_id, rover_id=rover_id, mission_id=header.get("mission_id"))
                self._send_ack_immediate(ack_env, addr)
            else:
                logger.warning(f"Unhandled message_type: {mtype}")
        except Exception as e:
            logger.error(f"Error handling message {msg_id} from {addr}: {e}")
            traceback.print_exc()

    # -------------------------
    # Handlers for message types
    # -------------------------
    def _handle_request_mission(self, envelope: Dict[str, Any], addr):
        header = envelope["header"]
        body = envelope["body"]
        rover_id = header.get("rover_id") or f"{addr}"
        logger.info(f"Handling REQUEST_MISSION from {rover_id} ({addr})")

        # Simple policy: pick a pending mission from mission_store
        pending = self.mission_store.get_pending_mission_for_rover(rover_id)
        if pending is None:
            err_body = {"code": "E-NO-MISSION", "description": "No mission available"}
            err_env = ml_schema.build_envelope("ERROR", body=err_body, rover_id=rover_id)
            self._send_with_reliability(err_env, addr)
            return

        # Assign mission
        try:
            self.mission_store.assign_mission_to_rover(pending, rover_id)
        except Exception:
            logger.exception("Failed to assign mission in mission_store")

        ui = pending.get("update_interval_s") or config.DEFAULT_UPDATE_INTERVAL_S

        assign_body = {
            "mission_id": pending["mission_id"],
            "area": pending.get("area"),
            "task": pending.get("task"),
            "params": pending.get("params", {}),
            "max_duration_s": pending.get("max_duration_s"),
            "update_interval_s": ui,
            "priority": pending.get("priority", 1),
        }
        assign_env = ml_schema.build_envelope(
            "MISSION_ASSIGN",
            body=assign_body,
            rover_id=rover_id,
            mission_id=pending["mission_id"],
            seq=0,
        )
        logger.info(f"Sending MISSION_ASSIGN {pending['mission_id']} to {rover_id}@{addr}")
        self._send_with_reliability(assign_env, addr)

    def _handle_progress(self, envelope: Dict[str, Any], addr):
        header = envelope["header"]
        body = envelope["body"]
        rover_id = header.get("rover_id") or str(addr)
        mission_id = header.get("mission_id") or body.get("mission_id")
        logger.info(f"Progress from {rover_id} on mission {mission_id}: {body.get('progress_pct')}")
        try:
            if mission_id:
                self.mission_store.update_progress(mission_id, rover_id, body)
        except Exception:
            logger.exception("mission_store.update_progress failed")

    def _handle_complete(self, envelope: Dict[str, Any], addr):
        header = envelope["header"]
        body = envelope["body"]
        rover_id = header.get("rover_id") or str(addr)
        mission_id = header.get("mission_id") or body.get("mission_id")
        logger.info(f"MISSION_COMPLETE from {rover_id} for mission {mission_id}: {body.get('result')}")
        try:
            if mission_id:
                self.mission_store.complete_mission(mission_id, rover_id, body)
        except Exception:
            logger.exception("mission_store.complete_mission failed")

    # -------------------------
    # Sending utilities
    # -------------------------
    def _send_ack_immediate(self, ack_env: Dict[str, Any], addr):
        try:
            packet = ml_schema.envelope_to_bytes(ack_env)
            if self.transport:
                self.transport.sendto(packet, addr)
            acked = ack_env["body"].get("acked_msg_id")
            if acked:
                self.last_acks[acked] = packet
            logger.debug(f"Sent immediate ACK for {acked} to {addr}")
        except Exception as e:
            logger.exception(f"Failed to send immediate ACK to {addr}: {e}")

    def _send_with_reliability(self, env: Dict[str, Any], addr: Tuple[str, int]):
        try:
            packet = ml_schema.envelope_to_bytes(env)
        except Exception as e:
            logger.exception(f"Failed to serialize envelope for sending: {e}")
            return

        header = env["header"]
        msg_id = header["msg_id"]
        msg_type = header.get("message_type")
        mission_id = header.get("mission_id")

        now = time.time()
        timeout = config.TIMEOUT_TX_INITIAL
        po = PendingOutgoing(msg_id=msg_id, packet=packet, addr=addr, created_at=now, timeout_s=timeout,
                             message_type=msg_type, mission_id=mission_id)
        self.pending_outgoing[msg_id] = po

        try:
            if self.transport:
                self.transport.sendto(packet, addr)
            po.attempts += 1
            logger.debug(f"Sent msg_id={msg_id} to {addr} (attempt {po.attempts})")
            self.metrics.incr("messages_sent")
        except Exception:
            logger.exception(f"Failed to send packet to {addr}")

    def send_mission_cancel(self, mission_id: str, reason: Optional[str] = None) -> Optional[str]:
        """
        Request the assigned rover to cancel a mission by sending MISSION_CANCEL reliably.
        Returns the msg_id of the pending cancel (if sent) or None if no assigned rover/address known.
        """
        m = self.mission_store.get_mission(mission_id)
        if not m:
            logger.warning(f"send_mission_cancel: unknown mission {mission_id}")
            return None
        assigned = m.get("assigned_rover")
        if not assigned:
            logger.info(f"send_mission_cancel: mission {mission_id} not assigned; marking cancelled locally")
            try:
                self.mission_store.cancel_mission(mission_id, reason=reason or "cancel_requested")
            except Exception:
                logger.exception("mission_store.cancel_mission failed")
            return None

        # find rover address (mission_store should have registered rover info)
        rover_info = None
        try:
            rover_info = self.mission_store.get_rover(assigned)
        except Exception:
            logger.exception("mission_store.get_rover failed")

        if not rover_info:
            logger.warning(f"send_mission_cancel: no rover info for {assigned}, marking cancelled locally")
            try:
                self.mission_store.cancel_mission(mission_id, reason=reason or "cancel_requested")
            except Exception:
                logger.exception("mission_store.cancel_mission failed")
            return None

        addr = (rover_info.get("address", {}).get("ip"), int(rover_info.get("address", {}).get("port")))
        if not addr or not addr[0]:
            logger.warning(f"send_mission_cancel: invalid address for rover {assigned}: {rover_info.get('address')}")
            try:
                self.mission_store.cancel_mission(mission_id, reason=reason or "cancel_requested")
            except Exception:
                logger.exception("mission_store.cancel_mission failed")
            return None

        body = {"mission_id": mission_id, "reason": reason}
        env = ml_schema.build_envelope("MISSION_CANCEL", body=body, rover_id=None, mission_id=mission_id)
        logger.info(f"Sending MISSION_CANCEL for {mission_id} to {assigned}@{addr}")
        self._send_with_reliability(env, addr)
        return env["header"]["msg_id"]

    def _handle_incoming_ack(self, acked_msg_id: str, addr):
        """
        Trata ACK recebido pelo servidor, removendo pending_outgoing e registando métricas.
        Também aplica ações específicas quando o ACK confirma um MISSION_CANCEL.
        """
        po = self.pending_outgoing.pop(acked_msg_id, None)
        if po:
            elapsed = time.time() - po.created_at
            logger.info(f"Received ACK for {acked_msg_id} from {addr} (attempts={po.attempts} elapsed={elapsed:.2f}s)")
            self.metrics.incr("acks_received")
            # if this ACK acknowledges a cancel we sent, mark mission cancelled in mission_store
            try:
                if po.message_type == "MISSION_CANCEL" and po.mission_id:
                    try:
                        self.mission_store.cancel_mission(po.mission_id, reason="cancel_ack_received")
                        logger.info(f"Mission {po.mission_id} marked CANCELLED after ACK from {addr}")
                    except Exception:
                        logger.exception("mission_store.cancel_mission failed on cancel ACK")
            except Exception:
                logger.exception("Error handling special ACK case")
        else:
            logger.debug(f"Received ACK for unknown or already-handled msg {acked_msg_id}")

    async def _retransmit_loop(self):
        try:
            while True:
                now = time.time()
                to_remove = []
                for msg_id, po in list(self.pending_outgoing.items()):
                    if now >= po.next_timeout:
                        if po.attempts <= config.N_RETX:
                            try:
                                if self.transport:
                                    self.transport.sendto(po.packet, po.addr)
                                po.attempts += 1
                                po.next_timeout = now + (po.timeout_s * (config.BACKOFF_FACTOR ** (po.attempts - 1)))
                                logger.warning(f"Retransmit msg {msg_id} to {po.addr} attempt={po.attempts}")
                                self.metrics.incr("retransmits")
                            except Exception:
                                logger.exception(f"Failed retransmit for {msg_id}")
                        else:
                            logger.error(f"Retries exhausted for msg {msg_id} to {po.addr} (type={po.message_type})")
                            try:
                                if po.message_type == "MISSION_ASSIGN" and po.mission_id:
                                    if hasattr(self.mission_store, "unassign_mission"):
                                        self.mission_store.unassign_mission(po.mission_id, reason="undeliverable_assign")
                                        logger.info(f"Reverted assignment of mission {po.mission_id} after undeliverable ASSIGN to {po.addr}")
                                    else:
                                        lock = getattr(self.mission_store, "_lock", contextlib.nullcontext())
                                        with lock:
                                            m = getattr(self.mission_store, "_missions", {}).get(po.mission_id)
                                            if m:
                                                prev = m.get("assigned_rover")
                                                m["assigned_rover"] = None
                                                m["state"] = "CREATED"
                                                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "ASSIGN_FAILED", "reason": "undeliverable_assign"})
                                                if prev:
                                                    getattr(self.mission_store, "_rovers", {}).setdefault(prev, {})["state"] = "IDLE"
                                        try:
                                            if hasattr(self.mission_store, "_emit"):
                                                self.mission_store._emit("mission_assign_failed", {"mission_id": po.mission_id, "reason": "undeliverable_assign"})
                                        except Exception:
                                            pass
                                        logger.info(f"Reverted assignment (fallback) of mission {po.mission_id}")
                                    self.metrics.incr("assign_failures")
                                else:
                                    try:
                                        if hasattr(self.mission_store, "_emit"):
                                            self.mission_store._emit("outgoing_failed", {"msg_id": msg_id, "addr": po.addr, "message_type": po.message_type})
                                    except Exception:
                                        logger.exception("Failed to emit outgoing_failed")
                                    self.metrics.incr("outgoing_failures")
                            except Exception:
                                logger.exception("Error in exhausted-retry policy handler")
                            to_remove.append(msg_id)
                for mid in to_remove:
                    self.pending_outgoing.pop(mid, None)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info("_retransmit_loop cancelled")
        except Exception:
            logger.exception("Error in _retransmit_loop")
            raise

    async def _cleanup_loop(self):
        try:
            while True:
                now = time.time()
                cutoff = now - config.DEDUPE_RETENTION_S
                old = [mid for mid, ts in self.seen_msgs.items() if ts < cutoff]
                for mid in old:
                    self.seen_msgs.pop(mid, None)
                await asyncio.sleep(config.DEDUPE_CLEANUP_INTERVAL_S)
        except asyncio.CancelledError:
            logger.info("_cleanup_loop cancelled")
        except Exception:
            logger.exception("Error in _cleanup_loop")
            raise


# -------------------------
# Entrypoint
# -------------------------
def run_server(listen_host: str = "0.0.0.0", listen_port: int = None):
    if listen_port is None:
        listen_port = config.ML_UDP_PORT
    loop = asyncio.get_event_loop()
    mission_store = MissionStore()
    proto = MLServerProtocol(mission_store)
    listen = loop.create_datagram_endpoint(lambda: proto, local_addr=(listen_host, listen_port))
    transport, _ = loop.run_until_complete(listen)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down ML server (KeyboardInterrupt)")
    finally:
        transport.close()
        if proto._task_retransmit:
            proto._task_retransmit.cancel()
        if proto._task_cleanup:
            proto._task_cleanup.cancel()
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()


if __name__ == "__main__":
    run_server()