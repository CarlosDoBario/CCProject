"""
ml_client.py
Implementação do MissionLink (ML) UDP client (Rover) integrada com o roverSim

Funcionalidades nesta versão:
- Integração de RoverSim para simulação realista de movimento 3D, consumo de energia,
  execução de tasks (collect_samples, capture_images, env_analysis) e injeção de falhas.
- Envio de REQUEST_MISSION e recepção de MISSION_ASSIGN (com ACK).
- Envio periódico de PROGRESS usando a telemetria gerada por RoverSim (com pending/retransmit).
- Envio de ERROR quando RoverSim reporta falha.
- Envio de MISSION_COMPLETE quando RoverSim indica conclusão.
- Pending/retransmit, deduplicação e limpeza de caches (como na versão anterior).

Como usar:
  PYTHONPATH=src python3 src/rover/ml_client.py --rover-id R-001 --server 127.0.0.1 --port 50000

Integração:
- Ao receber um MISSION_ASSIGN, o cliente cria uma instância RoverSim (ou reutiliza uma existente)
  e chama rover_sim.start_mission(assign_body).
- O loop de progresso chama rover_sim.step(update_interval_s) e usa rover_sim.get_telemetry() para
  construir o body do PROGRESS. Se rover_sim.last_error estiver presente, envia ERROR.
- Quando rover_sim.is_mission_complete() for True, envia MISSION_COMPLETE com campos reais
  (samples_collected, info).

Observações:
- Requer src/rover/rover_sim.py presente (fornecido).
- Mantém compatibilidade com o servidor ml_server.py e mission_store.
"""

import asyncio
import time
from typing import Dict, Any, Optional, Tuple
import traceback

from common import ml_schema, config, utils
from rover.rover_sim import RoverSim

logger = utils.get_logger("ml.client")

# --- Pending and helper classes (same concept as previous version) -----


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

        # rover simulator (created on assign)
        self.rover_sim: Optional[RoverSim] = None

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
        Usa pending/retransmit para assegurar entrega (but request itself here not tracked with ACK).
        """
        body = {
            "capabilities": ["sampling", "imaging", "env"],
            "battery_level_pct": 100,
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "status": "idle",
        }
        env = ml_schema.build_envelope("REQUEST_MISSION", body=body, rover_id=self.rover_id)
        # We can choose to expect ACK for request, but server will reply with ASSIGN or ERROR
        self._enqueue_and_send(env, server_addr, expect_ack=False)
        # Note: server will respond with MISSION_ASSIGN, which our datagram_received handles

    async def _handle_assign(self, envelope: Dict[str, Any], addr: Tuple[str, int]):
        """
        Processa MISSION_ASSIGN: cria/atualiza RoverSim, inicia loop de progressos que usa o simulador.
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
        # create or reset rover_sim
        if self.rover_sim is None:
            # instantiate with current position (0,0,0) or could load persisted pos
            self.rover_sim = RoverSim(self.rover_id, position=(0.0, 0.0, 0.0))
        # prepare mission_spec expected by RoverSim.start_mission
        mission_spec = {
            "mission_id": mission_id,
            "area": body.get("area"),
            "task": body.get("task"),
            "params": body.get("params", {}),
            "update_interval_s": body.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S),
        }
        self.rover_sim.start_mission(mission_spec)

        # start progress loop (cancel previous if any)
        if self.progress_task and not self.progress_task.done():
            self.progress_task.cancel()
        loop = asyncio.get_event_loop()
        self.progress_task = loop.create_task(self._progress_loop(addr))

    async def _progress_loop(self, server_addr: Tuple[str, int]):
        """
        Envia PROGRESS periodicamente usando telemetria do RoverSim.
        Também gere envio de ERROR e MISSION_COMPLETE conforme o estado do simulador.
        """
        if not self.rover_sim or not self.current_mission:
            logger.warning("Progress loop started without rover_sim or current_mission")
            return

        update_interval = self.current_mission.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S)
        try:
            while self.current_mission:
                # advance simulation by update_interval seconds (can be replaced by smaller steps if desired)
                self.rover_sim.step(update_interval)
                tel = self.rover_sim.get_telemetry()
                # Build PROGRESS envelope from telemetry (body fields compatible)
                body = {
                    "mission_id": tel.get("mission_id"),
                    "progress_pct": tel.get("progress_pct"),
                    "position": tel.get("position"),
                    "battery_level_pct": tel.get("battery_level_pct"),
                    "status": tel.get("status"),
                    "errors": tel.get("last_error") and [tel.get("last_error")] or [],
                    "samples_collected": tel.get("samples_collected", 0),
                }
                env = ml_schema.build_envelope("PROGRESS", body=body, rover_id=self.rover_id, mission_id=tel.get("mission_id"))
                # send and expect ACK for progress
                self._enqueue_and_send(env, server_addr, expect_ack=True)

                # If simulator reported an error, send ERROR envelope (non-blocking)
                if tel.get("last_error"):
                    err_body = {
                        "code": tel["last_error"].get("code"),
                        "description": tel["last_error"].get("description"),
                        "severity": "critical",
                    }
                    err_env = ml_schema.build_envelope("ERROR", body=err_body, rover_id=self.rover_id, mission_id=tel.get("mission_id"))
                    # send error but do not block on ACK here (we can expect ACK)
                    self._enqueue_and_send(err_env, server_addr, expect_ack=True)
                    # After reporting an error, break or let the server instruct next steps. Here we break progress loop.
                    # The server may respond with MISSION_CANCEL or other instruction.
                    logger.info("Reported ERROR to server, halting progress loop pending server instruction")
                    # keep current_mission but exit loop to avoid duplicate error reports
                    break

                # If mission completed in simulator, send MISSION_COMPLETE
                if self.rover_sim.is_mission_complete():
                    # prepare complete body with realistic info
                    complete_body = {
                        "mission_id": tel.get("mission_id"),
                        "result": "success",
                        "samples_collected": tel.get("samples_collected", 0),
                        "info": "simulated completion",
                    }
                    complete_env = ml_schema.build_envelope("MISSION_COMPLETE", body=complete_body, rover_id=self.rover_id, mission_id=tel.get("mission_id"))
                    self._enqueue_and_send(complete_env, server_addr, expect_ack=True)
                    # clear mission state locally
                    self.current_mission = None
                    logger.info("Mission complete sent to server")
                    break

                # sleep until next update interval (allowing other tasks to run)
                await asyncio.sleep(update_interval)
        except asyncio.CancelledError:
            logger.info("Progress loop cancelled")
        except Exception:
            logger.exception("Error in progress loop")

    def _last_progress_pct(self, mission_id: str) -> float:
        # deprecated now that RoverSim produces telemetry
        return 0.0

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
    parser = argparse.ArgumentParser(description="MissionLink ML client (Rover) with RoverSim")
    parser.add_argument("--rover-id", required=True, help="Rover identifier (e.g. R-001)")
    parser.add_argument("--server", default="127.0.0.1", help="Nave-Mãe IP/hostname")
    parser.add_argument("--port", type=int, default=config.ML_UDP_PORT, help="Nave-Mãe ML UDP port")
    args = parser.parse_args()
    try:
        asyncio.run(main(args))
    except Exception:
        traceback.print_exc()