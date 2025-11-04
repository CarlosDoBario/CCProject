#!/usr/bin/env python3
"""
ml_server.py
Implementação MissionLink (ML) UDP server (Nave-Mãe)

Funcionalidades implementadas nesta versão:
- Asyncio UDP server (DatagramProtocol) para receber envelopes ML.
- Parse/validação de envelopes usando common/ml_schema.
- Deduplicação de mensagens recebidas (cache com retenção).
- Envio de ACKs automáticos para mensagens recebidas que requerem confirmação.
- Mecanismo de envio confiável para mensagens emitidas pelo servidor (pending_outgoing
  com retransmissão exponencial até N_RETX).
- Integração mínima com mission_store (interface esperada documentada abaixo).
- Logging e métricas simples.

Observações:
- Requer os ficheiros em /src/common (ml_schema.py, config.py, utils.py).
- Espera encontrar um módulo nave_mae.mission_store com a classe MissionStore.
  Se não existir, o módulo cria um mission_store mínimo local (in-memory) para propósitos
  de desenvolvimento/testes.
- Para testes locais podes executar este ficheiro e depois usar o rover client para interagir.

Como usar (exemplo rápido):
  PYTHONPATH=src python3 src/nave_mae/ml_server.py

O servidor escuta na porta configurada em common.config.ML_UDP_PORT (default 50000).
"""

import asyncio
import time
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
        API methods used by ml_server:
         - register_rover(rover_id, address)
         - get_pending_mission_for_rover(rover_id) -> mission dict or None
         - assign_mission_to_rover(mission, rover_id)
         - update_progress(mission_id, rover_id, progress_payload)
         - complete_mission(mission_id, rover_id, result_payload)
         - create_mission(mission_spec) -> mission_id
         - list_missions()
        This simple store does NOT persist data.
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
            # return first unassigned mission
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


class PendingOutgoing:
    """
    Representa uma mensagem enviada pelo servidor que aguarda ACK.
    """
    def __init__(self, msg_id: str, packet: bytes, addr: Tuple[str, int], created_at: float, timeout_s: float):
        self.msg_id = msg_id
        self.packet = packet
        self.addr = addr
        self.created_at = created_at
        self.next_timeout = created_at + timeout_s
        self.attempts = 0
        self.timeout_s = timeout_s


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
                # optional RTT measurement; if body contains nonce, reply ack
                nonce = body.get("nonce")
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
            # No mission available: reply with ERROR or a MISSION_ASSIGN with empty body?
            err_body = {"code": "E-NO-MISSION", "description": "No mission available"}
            err_env = ml_schema.build_envelope("ERROR", body=err_body, rover_id=rover_id)
            self._send_with_reliability(err_env, addr)
            return

        # Assign mission
        try:
            self.mission_store.assign_mission_to_rover(pending, rover_id)
        except Exception:
            logger.exception("Failed to assign mission in mission_store")

        # Build MISSION_ASSIGN envelope using mission contents
        assign_body = {
            "mission_id": pending["mission_id"],
            "area": pending.get("area"),
            "task": pending.get("task"),
            "params": pending.get("params", {}),
            "max_duration_s": pending.get("max_duration_s"),
            "update_interval_s": pending.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S),
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
        logger.info(f"MISSION_COMPLETE from {rover_id} for mission {mission_id} result={body.get('result')}")
        try:
            if mission_id:
                self.mission_store.complete_mission(mission_id, rover_id, body)
        except Exception:
            logger.exception("mission_store.complete_mission failed")

    # -------------------------
    # Sending utilities
    # -------------------------
    def _send_ack_immediate(self, ack_env: Dict[str, Any], addr):
        """
        Envia um ACK imediatamente (não sujeito a retransmit logic do servidor).
        Guarda o ack packet para resposta a duplicados.
        """
        try:
            packet = ml_schema.envelope_to_bytes(ack_env)
            self.transport.sendto(packet, addr)
            # record last ack for the referenced msg_id (body.acked_msg_id)
            acked = ack_env["body"].get("acked_msg_id")
            if acked:
                self.last_acks[acked] = packet
            logger.debug(f"Sent immediate ACK for {acked} to {addr}")
        except Exception as e:
            logger.exception(f"Failed to send immediate ACK to {addr}: {e}")

    def _send_with_reliability(self, env: Dict[str, Any], addr: Tuple[str, int]):
        """
        Send a message and track it in pending_outgoing until an ACK is received or retries exhausted.
        """
        try:
            packet = ml_schema.envelope_to_bytes(env)
        except Exception as e:
            logger.exception(f"Failed to serialize envelope for sending: {e}")
            return

        header = env["header"]
        msg_id = header["msg_id"]

        # Create pending entry
        now = time.time()
        timeout = config.TIMEOUT_TX_INITIAL
        po = PendingOutgoing(msg_id=msg_id, packet=packet, addr=addr, created_at=now, timeout_s=timeout)
        self.pending_outgoing[msg_id] = po

        # send first time immediately
        try:
            self.transport.sendto(packet, addr)
            po.attempts += 1
            logger.debug(f"Sent msg_id={msg_id} to {addr} (attempt {po.attempts})")
            self.metrics.incr("messages_sent")
        except Exception:
            logger.exception(f"Failed to send packet to {addr}")

    def _handle_incoming_ack(self, acked_msg_id: str, addr):
        """
        Trata ACK recebido pelo servidor, removendo pending_outgoing e registando métricas.
        """
        po = self.pending_outgoing.pop(acked_msg_id, None)
        if po:
            elapsed = time.time() - po.created_at
            logger.info(f"Received ACK for {acked_msg_id} from {addr} (attempts={po.attempts} elapsed={elapsed:.2f}s)")
            self.metrics.incr("acks_received")
        else:
            logger.debug(f"Received ACK for unknown or already-handled msg {acked_msg_id}")

    # -------------------------
    # Background tasks
    # -------------------------
    async def _retransmit_loop(self):
        """
        Periodic task that checks pending_outgoing for timeouts and retransmits.
        """
        try:
            while True:
                now = time.time()
                to_remove = []
                for msg_id, po in list(self.pending_outgoing.items()):
                    if now >= po.next_timeout:
                        if po.attempts <= config.N_RETX:
                            # retransmit
                            try:
                                self.transport.sendto(po.packet, po.addr)
                                po.attempts += 1
                                # update next timeout with exponential backoff
                                po.next_timeout = now + (po.timeout_s * (config.BACKOFF_FACTOR ** (po.attempts - 1)))
                                logger.warning(f"Retransmit msg {msg_id} to {po.addr} attempt={po.attempts}")
                                self.metrics.incr("retransmits")
                            except Exception:
                                logger.exception(f"Failed retransmit for {msg_id}")
                        else:
                            # exhausted retries
                            logger.error(f"Retries exhausted for msg {msg_id} to {po.addr}")
                            to_remove.append(msg_id)
                            # Optional: notify mission_store or escalate
                # remove expired
                for mid in to_remove:
                    self.pending_outgoing.pop(mid, None)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info("_retransmit_loop cancelled")
        except Exception:
            logger.exception("Error in _retransmit_loop")
            raise

    async def _cleanup_loop(self):
        """
        Periodic cleanup (dedupe cache, old pending entries).
        """
        try:
            while True:
                now = time.time()
                # cleanup seen_msgs
                cutoff = now - config.DEDUPE_RETENTION_S
                old = [mid for mid, ts in self.seen_msgs.items() if ts < cutoff]
                for mid in old:
                    self.seen_msgs.pop(mid, None)
                # Optionally cleanup very old pending entries (if attempts exhausted for too long)
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
        # cancel tasks
        if proto._task_retransmit:
            proto._task_retransmit.cancel()
        if proto._task_cleanup:
            proto._task_cleanup.cancel()
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()


if __name__ == "__main__":
    run_server()