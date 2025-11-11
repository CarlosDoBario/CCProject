#!/usr/bin/env python3
"""
ml_client.py
MissionLink (ML) UDP client (Rover) implementation integrated with RoverSim.

Atualizado: quando retransmits esgotam para PROGRESS/MISSION_COMPLETE, marcamos missão AT_RISK
e persistimos o pacote não entregue para possível recuperação. Adicionado:
- re-send automático dos pacotes persistidos ao enviar REQUEST_MISSION
- remoção do ficheiro persistido após recepção do ACK correspondente
"""

import asyncio
import time
import socket
import os
import glob
from typing import Dict, Any, Optional, Tuple
import traceback

from common import ml_schema, config, utils
from rover.rover_sim import RoverSim

logger = utils.get_logger("ml.client")

class PendingSend:
    def __init__(self, msg_id: str, packet: bytes, addr: Tuple[str, int], created_at: float, timeout_s: float, message_type: str):
        self.msg_id = msg_id
        self.packet = packet
        self.addr = addr
        self.created_at = created_at
        self.next_timeout = created_at + timeout_s
        self.attempts = 0
        self.base_timeout = timeout_s
        self.message_type = message_type


class MLClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, rover_id: str, exit_on_complete: bool = False, done_event: Optional[asyncio.Event] = None):
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

        # simple backoff generator for re-requesting mission on failures
        self._re_request_backoff = utils.exponential_backoff(base=1.0, factor=2.0, max_delay=30.0)

        # exit-on-complete behavior: if True, set done_event when the mission completes (if requested)
        self.exit_on_complete = exit_on_complete
        self.done_event = done_event

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
            elif mtype == "MISSION_CANCEL":
                # stop mission immediately
                logger.info(f"Received MISSION_CANCEL for mission {header.get('mission_id')}: stopping local execution")
                self._handle_mission_cancel(header.get("mission_id"))
                ack_env = ml_schema.make_ack(msg_id, rover_id=self.rover_id, mission_id=header.get("mission_id"))
                self._send_ack_immediate(ack_env, addr)
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
        msg_type = header.get("message_type", "UNKNOWN")
        now = time.time()
        timeout = config.TIMEOUT_TX_INITIAL
        pending = PendingSend(msg_id=msg_id, packet=packet, addr=addr, created_at=now, timeout_s=timeout, message_type=msg_type)
        pending.attempts = 1
        pending.next_timeout = now + timeout
        if expect_ack:
            self.pending[msg_id] = pending
        # send immediately
        self._send_packet(packet, addr)
        logger.debug(f"Sent {msg_type} msg_id={msg_id} to {addr} (attempt 1)")

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
        Envia REQUEST_MISSION; does not expect ACK but server will reply with ASSIGN/ERROR.
        Also attempts to resend any persisted undelivered packets for this rover to server_addr.
        """
        # Attempt to resend persisted packets before requesting a new mission
        try:
            self._resend_persisted_packets(server_addr)
        except Exception:
            logger.exception("Error while resending persisted packets (continuing to request mission)")

        body = {
            "capabilities": ["sampling", "imaging", "env"],
            "battery_level_pct": 100,
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "status": "idle",
        }
        env = ml_schema.build_envelope("REQUEST_MISSION", body=body, rover_id=self.rover_id)
        self._enqueue_and_send(env, server_addr, expect_ack=False)

    def _resend_persisted_packets(self, server_addr: Tuple[str, int]) -> None:
        """
        Scan ML_CLIENT_CACHE_DIR (or default) for files matching {rover_id}-{msg_id}.bin
        and attempt to resend them to server_addr. Files are NOT deleted here; they are
        removed only when an ACK for the corresponding msg_id is received.
        """
        cache_dir = os.getenv("ML_CLIENT_CACHE_DIR", None) or os.path.join(os.path.expanduser("~"), ".ml_client_cache")
        if not os.path.isdir(cache_dir):
            # nothing to do
            return

        pattern = os.path.join(cache_dir, f"{self.rover_id}-*.bin")
        files = glob.glob(pattern)
        if not files:
            return

        logger.info(f"Found {len(files)} persisted packet(s) for rover {self.rover_id}, attempting resend to {server_addr}")
        for fpath in files:
            try:
                with open(fpath, "rb") as f:
                    packet = f.read()
                # Try to parse envelope to extract header info (msg_id, message_type)
                try:
                    env = ml_schema.parse_envelope(packet)
                    header = env["header"]
                    msg_id = header["msg_id"]
                    msg_type = header.get("message_type", "UNKNOWN")
                except Exception:
                    # If parsing fails, fallback to using filename to get msg_id
                    basename = os.path.basename(fpath)
                    # expecting format roverid-msgid.bin
                    parts = basename.rsplit("-", 1)
                    msg_id = parts[1].rsplit(".", 1)[0] if len(parts) == 2 else fpath
                    msg_type = "UNKNOWN"

                now = time.time()
                timeout = config.TIMEOUT_TX_INITIAL
                po = PendingSend(msg_id=msg_id, packet=packet, addr=server_addr, created_at=now, timeout_s=timeout, message_type=msg_type)
                po.attempts = 1
                po.next_timeout = now + timeout
                # register pending so retransmit loop can handle retries and ack removal will clean up file
                self.pending[msg_id] = po
                # send now
                try:
                    self.transport.sendto(packet, server_addr)
                    logger.info(f"Resent persisted packet {msg_id} to {server_addr}")
                except Exception:
                    logger.exception(f"Failed to resend persisted packet {msg_id} to {server_addr}")
            except Exception:
                logger.exception(f"Error while processing persisted file {fpath}")

    async def _handle_assign(self, envelope: Dict[str, Any], addr: Tuple[str, int]):
        header = envelope["header"]
        body = envelope["body"]
        mission_id = body.get("mission_id")
        # Coalesce update_interval_s to default if None
        ui = body.get("update_interval_s") or config.DEFAULT_UPDATE_INTERVAL_S
        logger.info(f"Mission assigned: {mission_id}, task={body.get('task')} params={body.get('params')}")
        self.current_mission = {
            "mission_id": mission_id,
            "task": body.get("task"),
            "params": body.get("params", {}),
            "update_interval_s": ui,
        }
        # create or reset rover_sim
        if self.rover_sim is None:
            self.rover_sim = RoverSim(self.rover_id, position=(0.0, 0.0, 0.0))
        mission_spec = {
            "mission_id": mission_id,
            "area": body.get("area"),
            "task": body.get("task"),
            "params": body.get("params", {}),
            "update_interval_s": ui,
        }
        self.rover_sim.start_mission(mission_spec)

        # start progress loop (cancel previous if any)
        if self.progress_task and not self.progress_task.done():
            self.progress_task.cancel()
        loop = asyncio.get_event_loop()
        self.progress_task = loop.create_task(self._progress_loop(addr))

    def _handle_mission_cancel(self, mission_id: Optional[str]) -> None:
        # Stop rover_sim and current mission if matches
        if self.current_mission and self.current_mission.get("mission_id") == mission_id:
            self.current_mission = None
            if self.progress_task and not self.progress_task.done():
                self.progress_task.cancel()
            if self.rover_sim:
                self.rover_sim.state = "IDLE"
            logger.info(f"Mission {mission_id} cancelled locally")
            # If exit_on_complete behavior is enabled, signal done_event so main can exit
            if self.exit_on_complete and self.done_event:
                try:
                    self.done_event.set()
                except Exception:
                    pass

    async def _progress_loop(self, server_addr: Tuple[str, int]):
        if not self.rover_sim or not self.current_mission:
            logger.warning("Progress loop started without rover_sim or current_mission")
            return

        update_interval = self.current_mission.get("update_interval_s", config.DEFAULT_UPDATE_INTERVAL_S)
        try:
            while self.current_mission:
                # ensure update_interval is numeric
                ui = update_interval or config.DEFAULT_UPDATE_INTERVAL_S
                self.rover_sim.step(ui)
                tel = self.rover_sim.get_telemetry()
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
                self._enqueue_and_send(env, server_addr, expect_ack=True)

                if tel.get("last_error"):
                    err_body = {
                        "code": tel["last_error"].get("code"),
                        "description": tel["last_error"].get("description"),
                        "severity": "critical",
                    }
                    err_env = ml_schema.build_envelope("ERROR", body=err_body, rover_id=self.rover_id, mission_id=tel.get("mission_id"))
                    self._enqueue_and_send(err_env, server_addr, expect_ack=True)
                    logger.info("Reported ERROR to server, halting progress loop pending server instruction")
                    # signal done_event if configured so tests can exit
                    if self.exit_on_complete and self.done_event:
                        try:
                            self.done_event.set()
                        except Exception:
                            pass
                    break

                if self.rover_sim.is_mission_complete():
                    complete_body = {
                        "mission_id": tel.get("mission_id"),
                        "result": "success",
                        "samples_collected": tel.get("samples_collected", 0),
                        "info": "simulated completion",
                    }
                    complete_env = ml_schema.build_envelope("MISSION_COMPLETE", body=complete_body, rover_id=self.rover_id, mission_id=tel.get("mission_id"))
                    self._enqueue_and_send(complete_env, server_addr, expect_ack=True)
                    self.current_mission = None
                    logger.info("Mission complete sent to server")
                    # If exit-on-complete requested, notify main to exit
                    if self.exit_on_complete and self.done_event:
                        try:
                            self.done_event.set()
                        except Exception:
                            pass
                    break

                await asyncio.sleep(ui)
        except asyncio.CancelledError:
            logger.info("Progress loop cancelled")
        except Exception:
            logger.exception("Error in progress loop")

    # -------------------------
    # Incoming ACK handling
    # -------------------------
    def _handle_incoming_ack(self, acked_msg_id: str):
        po = self.pending.pop(acked_msg_id, None)
        if po:
            elapsed = time.time() - po.created_at
            logger.info(f"Received ACK for {acked_msg_id} (attempts={po.attempts} elapsed={elapsed:.2f}s)")
            self.metrics.incr("acks_received")
            # If there was a persisted copy for this msg_id, remove it now
            try:
                cache_dir = os.getenv("ML_CLIENT_CACHE_DIR", None) or os.path.join(os.path.expanduser("~"), ".ml_client_cache")
                fname = os.path.join(cache_dir, f"{self.rover_id}-{acked_msg_id}.bin")
                if os.path.exists(fname):
                    try:
                        os.remove(fname)
                        logger.info(f"Removed persisted packet file {fname} after ACK {acked_msg_id}")
                    except Exception:
                        logger.exception(f"Failed to remove persisted packet file {fname}")
            except Exception:
                logger.exception("Error while trying to remove persisted packet after ACK")
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
                            try:
                                self.transport.sendto(po.packet, po.addr)
                                po.attempts += 1
                                po.next_timeout = now + (po.base_timeout * (config.BACKOFF_FACTOR ** (po.attempts - 1)))
                                logger.warning(f"Retransmit {msg_id} attempt={po.attempts}")
                                self.metrics.incr("retransmits")
                            except Exception:
                                logger.exception("Failed retransmit")
                        else:
                            # retries exhausted: handle failure
                            logger.error(f"Retries exhausted for {msg_id} (type={po.message_type})")
                            self.pending.pop(msg_id, None)
                            # handle failure according to message type
                            self._handle_send_failure(po)
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info("_retransmit_loop cancelled")
        except Exception:
            logger.exception("Error in retransmit loop")
            raise

    def _handle_send_failure(self, pending: PendingSend):
        """
        Simple fallback policy when a message exhausts retransmits:
         - If message_type is PROGRESS or MISSION_COMPLETE: log and try to re-request a mission after backoff.
         - Persist undelivered packet to disk for later inspection.
        """
        mtype = pending.message_type
        if mtype in ("PROGRESS", "MISSION_COMPLETE"):
            # mark local mission as at-risk
            try:
                if self.current_mission and self.current_mission.get("mission_id") == getattr(pending, "mission_id", None):
                    self.current_mission["state"] = "AT_RISK"
            except Exception:
                pass

            # persist last telemetry as fallback (simple file per rover)
            try:
                cache_dir = os.getenv("ML_CLIENT_CACHE_DIR", None) or os.path.join(os.path.expanduser("~"), ".ml_client_cache")
                os.makedirs(cache_dir, exist_ok=True)
                fname = os.path.join(cache_dir, f"{self.rover_id}-{pending.msg_id}.bin")
                with open(fname, "wb") as f:
                    f.write(pending.packet)
                logger.info(f"Persisted undelivered packet to {fname}")
            except Exception:
                logger.exception("Failed to persist undelivered packet")

            # try to re-request mission after backoff
            delay = next(self._re_request_backoff)
            logger.warning(f"On send-failure of {mtype}, will attempt to re-request mission after {delay:.1f}s")
            asyncio.get_event_loop().create_task(self._delayed_re_request(delay, pending.addr))
        elif mtype == "ERROR":
            logger.error("Critical ERROR message could not be delivered; logging and continuing")
            if self.exit_on_complete and self.done_event:
                try:
                    self.done_event.set()
                except Exception:
                    pass
        else:
            logger.error(f"Unacknowledged message {pending.msg_id} of type {mtype}")

    async def _delayed_re_request(self, delay_s: float, server_addr: Tuple[str, int]):
        await asyncio.sleep(delay_s)
        try:
            await self.request_mission(server_addr)
        except Exception:
            logger.exception("Failed re-requesting mission")

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
    exit_on_complete = args.exit_on_complete

    connect_addr = (server_host, server_port)
    logger.info(f"Starting ML client {rover_id} -> server {connect_addr} (exit_on_complete={exit_on_complete})")

    # create an Event the protocol will set when the mission completes (if requested)
    done_event: Optional[asyncio.Event] = asyncio.Event() if exit_on_complete else None

    # create protocol and endpoint; pass exit_on_complete and done_event to the protocol factory
    # Force IPv4 family and bind to loopback so the client receives replies from server reliably on Windows
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MLClientProtocol(rover_id, exit_on_complete=exit_on_complete, done_event=done_event),
        local_addr=('127.0.0.1', 0),
        family=socket.AF_INET,
    )
    proto: MLClientProtocol = protocol

    # request mission
    await proto.request_mission(connect_addr)

    # If exit_on_complete is requested, wait for done_event then exit; else run forever like before
    try:
        if exit_on_complete and done_event:
            # Wait until the protocol signals mission completion (or error)
            await done_event.wait()
            logger.info("Done event received; shutting down client (exit_on_complete)")
            # small grace to let pending ACK handling finish
            await asyncio.sleep(0.1)
        else:
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
    parser.add_argument("--exit-on-complete", action="store_true", help="Exit automatically when assigned mission completes (useful for tests)")
    args = parser.parse_args()
    try:
        asyncio.run(main(args))
    except Exception:
        traceback.print_exc()