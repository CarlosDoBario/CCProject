"""
ml_client.py
Implementação MissionLink (ML) UDP client (Rover)

Funcionalidades implementadas nesta versão:
- Cliente ML baseado em asyncio que:
  - envia REQUEST_MISSION com mecanismo de pending/retransmit;
  - recebe MISSION_ASSIGN, valida checksum e envia ACK;
  - mantém pending map para mensagens que exigem ACK;
  - envia PROGRESS periodicamente durante a execução da missão (com retransmit);
  - envia MISSION_COMPLETE e espera ACK;
  - lógica simples de deduplicação/reenviar ACKs para mensagens duplicadas.
- Integra com common.ml_schema, common.config e common.utils.

Como usar (exemplo rápido):
  PYTHONPATH=src python3 src/rover/ml_client.py --rover-id R-001 --server 127.0.0.1 --port 50000

Notas:
- Esta implementação é feita para facilitar testes locais e integração com o servidor ml_server.py.
- Para simulação completa (movimento, energia, falhas) integra com rover_sim.py (não incluído aqui).
"""

import asyncio
import time
from typing import Dict, Any, Optional, Tuple
import traceback

from common import ml_schema, config, utils

logger = utils.get_logger("ml.client")


class PendingSend:
    def __init__(self, msg_id: str, packet: bytes, addr: Tuple[str, int], created_at: float, timeout_s: float):
        self.msg_id = msg_id
        self.packet = packet
        self.addr = addr
        self.created_at = created_at
        self.next_timeout = created_at + timeout_s
        self.attempts = 0
        self.base_timeout = timeout_s


class MLClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, rover_id: str):
        self.transport = None
        self.rover_id = rover_id

        # pending messages that expect ACK
        self.pending: Dict[str, PendingSend] = {}

        # dedup seen inbound messages: msg_id -> timestamp
        self.seen_inbound: Dict[str, float] = {}

        # saved last ACK packets we sent back (for duplication)
        self.last_acks: Dict[str, bytes] = {}

        # mission state
        self.current_mission: Optional[Dict[str, Any]] = None
        self.progress_task = None

        # metrics
        self.metrics = utils.metrics

        # background task
        self._task_retransmit = None
        self._task_cleanup = None

    def connection_made(self, transport):
        self.transport = transport
        sockname = transport.get_extra_info("sockname")
        logger.info(f"ML client {self.rover_id} listening on {sockname}")
        loop = asyncio.get_event_loop()
        self._task_retransmit = loop.create_task(self._retransmit_loop())
        self._task_cleanup = loop.create_task(self._cleanup_loop())

    def connection_lost(self, exc):
        logger.info("Connection lost")
        if self._task_retransmit:
            self._task_retransmit.cancel()
        if self._task_cleanup:
            self._task_cleanup.cancel()

    def datagram_received(self, data: bytes, addr):
        try:
            envelope = ml_schema.parse_envelope(data)
        except Exception as e:
            logger.warning(f"Failed to parse envelope from {addr}: {e}")
            return

        header = envelope["header"]
        body = envelope["body"]
        msg_id = header["msg_id"]
        mtype = header["message_type"]
        sender_rover = header.get("rover_id")
        logger.debug(f"Received {mtype} msg_id={msg_id} from {addr} rover_id={sender_rover}")

        # dedup inbound
        if msg_id in self.seen_inbound:
            logger.debug(f"Duplicate inbound {msg_id} — resend ACK if we have it")
            ack_pkt = self.last_acks.get(msg_id)
            if ack_pkt:
                self.transport.sendto(ack_pkt, addr)
            return
        self.seen_inbound[msg_id] = time.time()

        try:
            if mtype == "MISSION_ASSIGN":
                # send ACK for assign
                ack_env = ml_schema.make_ack(msg_id, rover_id=self.rover_id, mission_id=header.get("mission_id"))
                self._send_ack_immediate(ack_env, addr)
                # process assign
                asyncio.get_event_loop().create_task(self._handle_assign(envelope, addr))
            elif mtype == "ERROR":
                logger.warning(f"Received ERROR from server: {body}")
            elif mtype == "ACK":
                acked = body.get("acked_msg_id")
                if acked:
                    self._handle_incoming_ack(acked)
            else:
                logger.info(f"Unhandled message_type {mtype} from {addr}")
        except Exception:
            logger.exception("Error processing inbound datagram")

    # -------------------------
    # Outbound helpers
    # -------------------------
    def _send_packet(self, packet: bytes, addr: Tuple[str, int]):
        try:
            self.transport.sendto(packet, addr)
            self.metrics.incr("messages_sent")
        except Exception:
            logger.exception("Failed to send packet")

    def _enqueue_and_send(self, env: Dict[str, Any], addr: Tuple[str, int], expect_ack: bool = True):
        """
        Envia um envelope e regista como pending se expect_ack == True.
        """
        try:
            packet = ml_schema.envelope_to_bytes(env)
        except Exception:
            logger.exception("Failed to serialize envelope")
            return

        header = env["header"]
        msg_id = header["msg_id"]
        now = time.time()
        timeout = config.TIMEOUT_TX_INITIAL
        pending = PendingSend(msg_id=msg_id, packet=packet, addr=addr, created_at=now, timeout_s=timeout)
        pending.attempts = 1
        pending.next_timeout = now + timeout
        if expect_ack:
            self.pending[msg_id] = pending
        # send immediately
        self._send_packet(packet, addr)
        logger.debug(f"Sent {header['message_type']} msg_id={msg_id} to {addr} (attempt 1)")

    def _send_ack_immediate(self, ack_env: Dict[str, Any], addr: Tuple[str, int]):
        """
        Envia ACK sem ser subject to pending retransmit logic. Guarda packet para re-sending on dup.
        """
        try:
            packet = ml_schema.envelope_to_bytes(ack_env)
            self.transport.sendto(packet, addr)
            # store last ack keyed by the original msg_id it acknowledges
            acked = ack_env["body"].get("acked_msg_id")
            if acked:
                self.last_acks[acked] = packet
            logger.debug(f"Sent immediate ACK for {acked} to {addr}")
        except Exception:
            logger.exception("Failed to send immediate ACK")

    # -------------------------
    # High-level actions
    # -------------------------
    async def request_mission(self, server_addr: Tuple[str, int]):
        """
        Envia REQUEST_MISSION e espera por MISSION_ASSIGN (ou ERROR).
        Usa pending/retransmit para assegurar entrega.
        """
        body = {
            "capabilities": ["sampling", "imaging", "env"],
            "battery_level_pct": 100,
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "status": "idle",
        }
        env = ml_schema.build_envelope("REQUEST_MISSION", body=body, rover_id=self.rover_id)
        self._enqueue_and_send(env, server_addr, expect_ack=False)
        # Note: server will respond with MISSION_ASSIGN, which our datagram_received handles

    async def _handle_assign(self, envelope: Dict[str, Any], addr: Tuple[str, int]):
        """
        Processa MISSION_ASSIGN: guarda mission, inicia loop de progressos.
        """
        header = envelope["header"]
        body = envelope["body"]
        mission_id = body.get("mission_id")
        logger.info(f"Mission assigned: {mission_id}, task={body.get('task')} params={body.get('params')}")
        # store mission
        self.current_mission = {
            "mission_id": mission_id,
            "task": body.get("task"),
            "params": body.get("params", {}),
            "update_interval_s": body.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S),
        }
        # start progress loop
        if self.progress_task and not self.progress_task.done():
            self.progress_task.cancel()
        loop = asyncio.get_event_loop()
        self.progress_task = loop.create_task(self._progress_loop(addr))

    async def _progress_loop(self, server_addr: Tuple[str, int]):
        """
        Envia PROGRESS periodicamente enquanto current_mission existir.
        Cada PROGRESS é sujeito a retransmissão (pending).
        """
        try:
            while self.current_mission:
                mission_id = self.current_mission["mission_id"]
                # For demo: increment progress_pct by random small value
                import random
                progress_pct = min(100.0, random.uniform(1.0, 5.0) + self._last_progress_pct(mission_id))
                body = {
                    "mission_id": mission_id,
                    "progress_pct": round(progress_pct, 2),
                    "position": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "battery_level_pct": max(0, 100 - int(progress_pct)),  # dummy battery drain
                    "status": "in_progress" if progress_pct < 100.0 else "completed",
                    "errors": [],
                }
                env = ml_schema.build_envelope("PROGRESS", body=body, rover_id=self.rover_id, mission_id=mission_id)
                # use pending retransmit (expect ACK)
                self._enqueue_and_send(env, server_addr, expect_ack=True)
                # if progress reached 100 -> send MISSION_COMPLETE and break
                if progress_pct >= 100.0:
                    # wait a short moment then send complete
                    await asyncio.sleep(0.5)
                    complete_body = {
                        "mission_id": mission_id,
                        "result": "success",
                        "samples_collected": 1,
                        "info": "simulated completion",
                    }
                    complete_env = ml_schema.build_envelope("MISSION_COMPLETE", body=complete_body, rover_id=self.rover_id, mission_id=mission_id)
                    self._enqueue_and_send(complete_env, server_addr, expect_ack=True)
                    # clear mission after sending complete
                    self.current_mission = None
                    break
                # sleep until next update
                await asyncio.sleep(self.current_mission.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S))
        except asyncio.CancelledError:
            logger.info("Progress loop cancelled")
        except Exception:
            logger.exception("Error in progress loop")

    def _last_progress_pct(self, mission_id: str) -> float:
        # naive: check pending progress messages for same mission to estimate last value
        last = 0.0
        for p in self.pending.values():
            try:
                envelope = ml_schema.parse_envelope(p.packet)
                h = envelope["header"]
                b = envelope["body"]
                if envelope["header"].get("message_type") == "PROGRESS" and b.get("mission_id") == mission_id:
                    last = max(last, float(b.get("progress_pct", 0.0)))
            except Exception:
                continue
        return last

    # -------------------------
    # Incoming ACK handling
    # -------------------------
    def _handle_incoming_ack(self, acked_msg_id: str):
        po = self.pending.pop(acked_msg_id, None)
        if po:
            elapsed = time.time() - po.created_at
            logger.info(f"Received ACK for {acked_msg_id} (attempts={po.attempts} elapsed={elapsed:.2f}s)")
            self.metrics.incr("acks_received")
        else:
            logger.debug(f"ACK for unknown msg {acked_msg_id}")

    # -------------------------
    # Retransmit & cleanup loops
    # -------------------------
    async def _retransmit_loop(self):
        try:
            while True:
                now = time.time()
                for msg_id, po in list(self.pending.items()):
                    if now >= po.next_timeout:
                        if po.attempts <= config.N_RETX:
                            # retransmit
                            try:
                                self.transport.sendto(po.packet, po.addr)
                                po.attempts += 1
                                # exponential backoff
                                po.next_timeout = now + (po.base_timeout * (config.BACKOFF_FACTOR ** (po.attempts - 1)))
                                logger.warning(f"Retransmit {msg_id} attempt={po.attempts}")
                                self.metrics.incr("retransmits")
                            except Exception:
                                logger.exception("Failed retransmit")
                        else:
                            # retries exhausted
                            logger.error(f"Retries exhausted for {msg_id}")
                            # remove pending and take fallback action (log / notify)
                            self.pending.pop(msg_id, None)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info("_retransmit_loop cancelled")
        except Exception:
            logger.exception("Error in retransmit loop")
            raise

    async def _cleanup_loop(self):
        try:
            while True:
                # cleanup seen_inbound
                cutoff = time.time() - config.DEDUPE_RETENTION_S
                old = [mid for mid, ts in self.seen_inbound.items() if ts < cutoff]
                for mid in old:
                    self.seen_inbound.pop(mid, None)
                await asyncio.sleep(config.DEDUPE_CLEANUP_INTERVAL_S)
        except asyncio.CancelledError:
            logger.info("_cleanup_loop cancelled")
        except Exception:
            logger.exception("Error in cleanup loop")
            raise


# -------------------------
# CLI / Entrypoint
# -------------------------
import argparse


async def main(args):
    loop = asyncio.get_event_loop()
    rover_id = args.rover_id
    server_host = args.server
    server_port = args.port

    connect_addr = (server_host, server_port)
    logger.info(f"Starting ML client {rover_id} -> server {connect_addr}")

    # create protocol and endpoint
    transport, protocol = await loop.create_datagram_endpoint(lambda: MLClientProtocol(rover_id), remote_addr=None)
    # When using sendto with transport created without remote_addr, supply addr in sendto.

    # Note: the protocol.transport is available now
    proto: MLClientProtocol = protocol

    # request mission
    await proto.request_mission(connect_addr)

    # keep running (progress loop will start when assign arrives)
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Client shutting down")
    finally:
        transport.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MissionLink ML client (Rover)")
    parser.add_argument("--rover-id", required=True, help="Rover identifier (e.g. R-001)")
    parser.add_argument("--server", default="127.0.0.1", help="Nave-Mãe IP/hostname")
    parser.add_argument("--port", type=int, default=config.ML_UDP_PORT, help="Nave-Mãe ML UDP port")
    args = parser.parse_args()
    try:
        asyncio.run(main(args))
    except Exception:
        traceback.print_exc()