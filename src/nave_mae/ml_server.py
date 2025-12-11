#!/usr/bin/env python3
"""
ml_server.py (binary ML/UDP version)
...
"""
from __future__ import annotations

import asyncio
import time
import logging
import struct
import threading
import json
import socket
from typing import Dict, Any, Optional, Tuple

from common import binary_proto, config, utils

logger = utils.get_logger("nave_mae.ml_server")


try:
    from nave_mae.mission_store import MissionStore  # type: ignore
except Exception:
    logger.warning("nave_mae.mission_store not available; using lightweight fallback")

    # Fallback leve
    class MissionStore:
        def __init__(self, persist_file: Optional[str] = None):
            self.missions = {}
            self.rovers = {}
            self._next_mission = 1

        def register_rover(self, rover_id, address=None):
            self.rovers[rover_id] = {"rover_id": rover_id, "address": address, "state": "IDLE", "last_seen": utils.now_iso()}

        def get_pending_mission_for_rover(self, rover_id):
            for m in self.missions.values():
                if m.get("assigned_rover") is None:
                    return m
            return None

        def assign_mission_to_rover(self, mission, rover_id):
            mission["assigned_rover"] = rover_id
            mission["state"] = "ASSIGNED"
            mission["assigned_at"] = utils.now_iso()

        def update_progress(self, mission_id, rover_id, payload):
            pass

        def complete_mission(self, mission_id, rover_id, payload):
            pass

        def unassign_mission(self, mission_id, reason=None):
            m = self.missions.get(mission_id)
            if m:
                m["assigned_rover"] = None
                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "UNASSIGNED", "reason": reason})

        def cancel_mission(self, mission_id, reason=None):
            m = self.missions.get(mission_id)
            if m:
                m["state"] = "CANCELLED"
                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL", "reason": reason})

        def get_mission(self, mission_id):
            return self.missions.get(mission_id)
        
        def create_demo_missions(self):
            demos = [
                {"task": "capture_images", "mission_id": "M-DEMO-IMG", "area": {"x1": 10, "y1": 10, "z1": 0, "x2": 20, "y2": 20, "z2": 0}, "params": {"interval_s": 5, "frames": 60, "max_duration_s": 20.0, "update_interval_s": 1.0}, "priority": 1},
                {"task": "collect_samples", "mission_id": "M-DEMO-SAM", "area": {"x1": 30, "y1": 5, "z1": 0, "x2": 35, "y2": 10, "z2": 0}, "params": {"depth_mm": 50, "sample_count": 2, "max_duration_s": 15.0, "update_interval_s": 1.0}, "priority": 2},
                {"task": "env_analysis", "mission_id": "M-DEMO-ENV", "area": {"x1": 0, "y1": 0, "z1": 0, "x2": 50, "y2": 50, "z2": 5}, "params": {"sensors": ["temp", "pressure"], "sampling_rate_s": 10, "max_duration_s": 20.0, "update_interval_s": 1.0}, "priority": 3},
            ]
            for m in demos:
                if m["mission_id"] not in self.missions:
                     self.missions[m["mission_id"]] = m
                     self.missions[m["mission_id"]]["state"] = "CREATED"


class PendingOutgoing:
    def __init__(
        self,
        msg_id: int,
        packet: bytes,
        addr: Tuple[str, int],
        created_at: float,
        timeout_s: float,
        message_type: Optional[str] = None,
        mission_id: Optional[str] = None,
    ):
        self.msg_id = int(msg_id)
        self.packet = packet
        self.addr = addr
        self.created_at = created_at
        self.next_timeout = created_at + timeout_s
        self.attempts = 0
        self.timeout_s = timeout_s
        self.message_type = message_type
        self.mission_id = mission_id
        self.metadata: Dict[str, Any] = {}


class MLServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, mission_store: MissionStore):
        super().__init__()
        self.transport = None
        self.mission_store = mission_store
        self.seen_msgs: Dict[int, float] = {}
        self.pending_outgoing: Dict[int, PendingOutgoing] = {}
        self.last_acks: Dict[int, bytes] = {}
        self.metrics = utils.metrics
        self._task_retransmit = None
        self._task_cleanup = None
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def connection_made(self, transport):
        self.transport = transport
        sockname = transport.get_extra_info("sockname")
        logger.info("ML server listening on %s", sockname)
        # É seguro chamar get_event_loop() aqui
        loop = asyncio.get_event_loop()
        self._loop = loop
        # schedule background maintenance tasks on this loop
        self._task_retransmit = loop.create_task(self._retransmit_loop())
        self._task_cleanup = loop.create_task(self._cleanup_loop())

    def connection_lost(self, exc):
        logger.info("ML server connection lost")
        # cancel background tasks if present
        if self._task_retransmit:
            self._task_retransmit.cancel()
        if self._task_cleanup:
            self._task_cleanup.cancel()

    def datagram_received(self, data: bytes, addr):
        try:
            parsed = binary_proto.parse_ml_datagram(data)
        except Exception as e:
            logger.warning("Failed to parse ML datagram from %s: %s", addr, e)
            # attempt to reply with an ERROR datagram
            try:
                err_tlv = (binary_proto.TLV_ERRORS, str(e).encode("utf-8"))
                err_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ERROR, "", [err_tlv])
                if self.transport:
                    self.transport.sendto(err_pkt, addr)
            except Exception:
                logger.exception("Failed to send ERROR datagram")
            return

        header = parsed.get("header", {})
        rover_id = parsed.get("rover_id", "")
        tlvmap = parsed.get("tlvs", {})
        canonical = binary_proto.tlv_to_canonical(tlvmap)
        msgid = int(header.get("msgid") or 0)

        # dedupe: if seen, resend last ack if available and skip processing
        if msgid in self.seen_msgs:
            last = self.last_acks.get(msgid)
            if last and self.transport:
                try:
                    self.transport.sendto(last, addr)
                except Exception:
                    logger.exception("Failed to re-send last ACK for duplicate msg %s", msgid)
            logger.debug("Duplicate ML msg %s from %s", msgid, addr)
            return
        self.seen_msgs[msgid] = time.time()

        # register rover address if present
        try:
            if rover_id:
                try:
                    # mission_store.register_rover might accept address dict or tuple
                    self.mission_store.register_rover(rover_id, address={"ip": addr[0], "port": int(addr[1])})
                    logger.info("Rover %s registered with address %s", rover_id, addr)
                except TypeError:
                    try:
                        self.mission_store.register_rover(rover_id, addr)
                    except Exception:
                        self.mission_store.register_rover(rover_id)
        except Exception:
            logger.exception("mission_store.register_rover failed (continuing)")

        mtype = header.get("msgtype")
        try:
            if mtype == binary_proto.ML_REQUEST_MISSION:
                self._handle_request_mission(rover_id, canonical, addr, msgid=msgid)

            elif mtype == binary_proto.ML_PROGRESS:
                self._handle_progress(rover_id, canonical, addr)
                # ack progress
                ack_pkt = binary_proto.pack_ml_datagram(
                    binary_proto.ML_ACK,
                    rover_id,
                    [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", msgid))],
                    msgid=0,
                )
                if self.transport:
                    self.transport.sendto(ack_pkt, addr)
                    self.last_acks[msgid] = ack_pkt

            elif mtype == binary_proto.ML_MISSION_COMPLETE:
                self._handle_complete(rover_id, canonical, addr)
                ack_pkt = binary_proto.pack_ml_datagram(
                    binary_proto.ML_ACK,
                    rover_id,
                    [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", msgid))],
                    msgid=0,
                )
                if self.transport:
                    self.transport.sendto(ack_pkt, addr)
                    self.last_acks[msgid] = ack_pkt

            elif mtype == binary_proto.ML_HEARTBEAT:
                # respond to heartbeat with ACK that acknowledges the heartbeat msgid
                try:
                    ack_pkt = binary_proto.pack_ml_datagram(
                        binary_proto.ML_ACK,
                        rover_id,
                        [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", msgid))],
                        msgid=0,
                    )
                    if self.transport:
                        self.transport.sendto(ack_pkt, addr)
                        self.last_acks[int(msgid)] = ack_pkt
                        logger.debug("Sent heartbeat ACK for msgid=%s to %s", msgid, addr)
                except Exception:
                    logger.exception("Failed to send ACK for HEARTBEAT")

            elif mtype == binary_proto.ML_ACK:
                # incoming ACK: check TLV ACKED_MSG_ID
                acked_bytes = tlvmap.get(binary_proto.TLV_ACKED_MSG_ID, [])
                acked_id = None
                if acked_bytes:
                    try:
                        acked_id = struct.unpack(">Q", acked_bytes[0])[0]
                    except Exception:
                        acked_id = None
                if acked_id is not None:
                    self._handle_incoming_ack(int(acked_id), addr)

            elif mtype == binary_proto.ML_ERROR:
                logger.warning("ML ERROR from %s@%s: %s", rover_id, addr, canonical.get("errors") or canonical)

            else:
                logger.warning("Unhandled ML msgtype %s from %s", mtype, addr)

        except Exception:
            logger.exception("Error handling ML message")

    # ----- handlers and helpers -------------------------------------------------
    def _handle_request_mission(self, rover_id: str, canonical: Dict[str, Any], addr: Tuple[str, int], msgid: int = 0):
        logger.info("Handling REQUEST_MISSION from %s (%s) [MsgID: %s]", rover_id, addr, msgid)
        
        # Primeiro, envia ACK para o REQUEST_MISSION recebido (para parar a retransmissão do cliente)
        try:
             ack_pkt_req = binary_proto.pack_ml_datagram(
                    binary_proto.ML_ACK,
                    rover_id,
                    [(binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", msgid))],
                    msgid=0,
                )
             if self.transport:
                self.transport.sendto(ack_pkt_req, addr)
                self.last_acks[msgid] = ack_pkt_req
                logger.debug("Sent ACK for REQUEST_MISSION msgid=%s", msgid)
        except Exception:
            logger.exception("Failed to send ACK for REQUEST_MISSION")


        pending = self.mission_store.get_pending_mission_for_rover(rover_id)
        if pending is None:
            err_tlv = (binary_proto.TLV_ERRORS, b"No mission available")
            err_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ERROR, rover_id, [err_tlv])
            if self.transport:
                self.transport.sendto(err_pkt, addr)
            logger.warning("No mission available for %s", rover_id)
            return
        
        try:
            self.mission_store.assign_mission_to_rover(pending, rover_id)
        except Exception:
            logger.exception("Failed to assign mission in mission_store")

        # build assign TLVs
        tlvs = []
        # Adiciona TLV_MISSION_ID e TLV_TASK
        tlvs.append((binary_proto.TLV_MISSION_ID, pending["mission_id"].encode("utf-8")))
        tlvs.append(binary_proto.tlv_string(binary_proto.TLV_TASK, pending["task"]))
        
        # Adiciona AREA se existir
        if pending.get("area"):
            area = pending["area"]
            vals = (float(area["x1"]), float(area["y1"]), float(area["z1"]), float(area["x2"]), float(area["y2"]), float(area["z2"]))
            area_bytes = struct.pack(">ffffff", *vals)
            tlvs.append((binary_proto.TLV_AREA, area_bytes))
        
        # Adiciona PARAMS_JSON
        params = pending.get("params", {})
        tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(params).encode("utf-8")))

        # numeric msgid for server->client message (uint64)
        server_msgid = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
        # MISSION_ASSIGN deve pedir ACK para garantir que o rover recebeu a missão
        assign_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_MISSION_ASSIGN, rover_id, tlvs, msgid=server_msgid, flags=binary_proto.FLAG_ACK_REQUESTED)

        # store pending for retransmit & track
        self._send_with_reliability(assign_pkt, addr, message_type="MISSION_ASSIGN", mission_id=pending["mission_id"], msgid=server_msgid)
        logger.info("Sent MISSION_ASSIGN [MsgID: %s] to %s@%s", server_msgid, rover_id, addr)


    def _handle_progress(self, rover_id: str, canonical: Dict[str, Any], addr: Tuple[str, int]):
        mission_id = canonical.get("mission_id")
        
        # Log da informação detalhada recebida (inclui temperatura/velocidade do TLV_PARAMS_JSON)
        progress_pct = canonical.get("progress_pct", 0.0)
        status = canonical.get("status", "N/A")
        temp = canonical.get("internal_temp_c", 0.0)
        speed = canonical.get("current_speed_m_s", 0.0)
        battery = canonical.get("battery_level_pct", 0.0)
        position = canonical.get("position", {"x": 0.0, "y": 0.0, "z": 0.0})
        
        logger.info(f"Progress from {rover_id} on mission {mission_id}: {progress_pct:.1f}%% (Status: {status}, Temp: {temp:.1f}°C, Speed: {speed:.1f}m/s)")

        try:
            if mission_id:
                self.mission_store.update_progress(mission_id, rover_id, canonical)
            
            # --- CORREÇÃO CRÍTICA: Atualiza o estado detalhado do Rover no MissionStore ---
            # O MissionStore deve ser a fonte de verdade para a API.
            with self.mission_store._lock:
                r = self.mission_store._rovers.setdefault(rover_id, {"rover_id": rover_id})
                
                # Atualiza os campos simples no nível superior para fácil acesso
                r["battery_level_pct"] = battery
                r["internal_temp_c"] = temp
                r["current_speed_m_s"] = speed
                r["position"] = position
                r["state"] = status # O status da telemetria é mais detalhado que o estado da missão
                
                # Armazena o payload completo sob uma chave para o API_Server ler
                r["last_telemetry_full"] = {
                    "status": status,
                    "battery_level_pct": battery,
                    "internal_temp_c": temp,
                    "current_speed_m_s": speed,
                    "position": position,
                    "timestamp_ms": canonical.get("timestamp_ms", utils.now_ms()),
                }
                
                # Salvar para garantir que o MissionStore do disco está atualizado
                self.mission_store.save_to_file()

        except Exception:
            logger.exception("mission_store update failed in ML Server")

    def _handle_complete(self, rover_id: str, canonical: Dict[str, Any], addr: Tuple[str, int]):
        mission_id = canonical.get("mission_id")
        logger.info("MISSION_COMPLETE from %s for mission %s", rover_id, mission_id)
        try:
            if mission_id:
                self.mission_store.complete_mission(mission_id, rover_id, canonical)
        except Exception:
            logger.exception("mission_store.complete_mission failed")

    def send_mission_cancel(self, mission_id: str, reason: Optional[str] = None) -> int:
        """
        Send a MISSION_CANCEL to the assigned rover for the given mission_id.
        Returns the numeric msgid used for the outgoing cancel (key for pending_outgoing).
        """
        m = self.mission_store.get_mission(mission_id)
        if not m:
            raise ValueError("Unknown mission_id")
        rover_id = m.get("assigned_rover")
        if not rover_id:
            raise ValueError("Mission not assigned")

        # Build TLVs: mission id and optional reason
        tlvs = [(binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8"))]
        if reason:
            tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps({"reason": reason}).encode("utf-8")))

        server_msgid = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
        # Use existing ML_CANCEL constant from binary_proto (compatibility with binary_proto naming)
        pkt = binary_proto.pack_ml_datagram(binary_proto.ML_CANCEL, rover_id, tlvs, msgid=server_msgid)

        # find rover address if registered
        addr = None
        try:
            rinfo = None
            if hasattr(self.mission_store, "get_rover"):
                rinfo = self.mission_store.get_rover(rover_id)
            if rinfo and isinstance(rinfo, dict) and "address" in rinfo and rinfo["address"]:
                addr = (rinfo["address"].get("ip"), int(rinfo["address"].get("port")))
        except Exception:
            addr = None

        if addr is None:
            # Assumir a porta do ML do config para o host padrão (é um chute, mas necessário)
            addr = (config.ML_HOST, config.ML_UDP_PORT)

        # store pending and send
        self._send_with_reliability(pkt, addr, message_type="MISSION_CANCEL", mission_id=mission_id, msgid=server_msgid)
        return server_msgid

    def _send_with_reliability(
        self,
        packet: bytes,
        addr: Tuple[str, int],
        message_type: str = None,
        mission_id: Optional[str] = None,
        msgid: Optional[int] = None,
    ):
        # respect datagram size limit
        if len(packet) > getattr(config, "ML_MAX_DATAGRAM_SIZE", 1200):
            logger.error("Attempt to send oversized ML datagram (%d bytes) to %s; dropping", len(packet), addr)
            return

        if msgid is None:
            try:
                hdr = binary_proto.unpack_ml_header(packet)
                msgid = int(hdr.get("msgid", int(time.time() * 1000)))
            except Exception:
                msgid = int(time.time() * 1000)

        now = time.time()
        timeout = config.TIMEOUT_TX_INITIAL
        po = PendingOutgoing(msg_id=msgid, packet=packet, addr=addr, created_at=now, timeout_s=timeout, message_type=message_type, mission_id=mission_id)
        po.attempts = 1
        po.next_timeout = now + timeout
        with self._lock:
            self.pending_outgoing[int(msgid)] = po
        try:
            self.transport.sendto(packet, addr)
            logger.debug("Sent %s msgid=%s to %s", message_type, msgid, addr)
            self.metrics.incr("messages_sent")
        except Exception:
            logger.exception("Failed to send packet to %s", addr)

    def _handle_incoming_ack(self, acked_msg_id: int, addr):
        with self._lock:
            po = self.pending_outgoing.pop(int(acked_msg_id), None)
        if po:
            logger.info("Received ACK for outgoing msg %s from %s (type=%s)", acked_msg_id, addr, po.message_type)
            self.metrics.incr("acks_received")
            # If this ACK confirms a cancel, instruct mission_store to mark the mission cancelled
            try:
                if po.message_type == "MISSION_CANCEL" and po.mission_id:
                    if hasattr(self.mission_store, "cancel_mission"):
                        self.mission_store.cancel_mission(po.mission_id, reason=po.metadata.get("reason") if po.metadata else None)
                    else:
                        # fallback: try to set state/history manually
                        try:
                            m = self.mission_store.get_mission(po.mission_id)
                            if m is not None:
                                m["state"] = "CANCELLED"
                                m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL"})
                                m["assigned_rover"] = None
                        except Exception:
                            logger.exception("Fallback cancel handling failed")
            except Exception:
                logger.exception("Failed to apply cancel action in mission_store for %s", po.mission_id)
        else:
            logger.debug("Received ACK for unknown/out-of-window msg %s from %s", acked_msg_id, addr)

    async def _retransmit_loop(self):
        try:
            while True:
                now = time.time()
                to_remove = []
                with self._lock:
                    items = list(self.pending_outgoing.items())
                for msg_id, po in items:
                    if now >= po.next_timeout:
                        if po.attempts <= config.N_RETX:
                            try:
                                if self.transport:
                                    self.transport.sendto(po.packet, po.addr)
                                po.attempts += 1
                                po.next_timeout = now + (po.timeout_s * (config.BACKOFF_FACTOR ** (po.attempts - 1)))
                                logger.warning("Retransmit msg %s to %s attempt=%d", msg_id, po.addr, po.attempts)
                                self.metrics.incr("retransmits")
                            except Exception:
                                logger.exception("Failed retransmit for %s", msg_id)
                        else:
                            logger.error("Retries exhausted for pending msg %s", msg_id)
                            try:
                                if po.message_type == "MISSION_ASSIGN" and po.mission_id:
                                    if hasattr(self.mission_store, "unassign_mission"):
                                        self.mission_store.unassign_mission(po.mission_id, reason="undeliverable_assign")
                                        logger.info("Reverted assignment of mission %s after undeliverable ASSIGN to %s", po.mission_id, po.addr)
                                    self.metrics.incr("assign_failures")
                                elif po.message_type == "MISSION_CANCEL" and po.mission_id:
                                    try:
                                        m = self.mission_store.get_mission(po.mission_id)
                                        if m is not None:
                                            m.setdefault("history", []).append({"ts": utils.now_iso(), "type": "CANCEL_FAILED", "reason": "undeliverable_cancel"})
                                            logger.info("Cancel failed for mission %s after undeliverable CANCEL to %s", po.mission_id, po.addr)
                                        self.metrics.incr("cancel_failures")
                                    except Exception:
                                        logger.exception("Error handling exhausted cancel")
                                else:
                                    self.metrics.incr("outgoing_failures")
                            except Exception:
                                logger.exception("Error in exhausted-retry policy handler")
                            to_remove.append(msg_id)
                with self._lock:
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

    # ----------------------------
    # New: graceful shutdown API
    # ----------------------------
    async def stop(self, wait_timeout: float = 2.0) -> None:
        """
        Gracefully stop the ML server protocol's background tasks and close the transport.
        This should be called from the event loop that created/owns the protocol (or awaited via run_coroutine_threadsafe).
        """
        logger.info("Stopping MLServerProtocol (graceful shutdown)")

        # Cancel retransmit/cleanup tasks and await their termination (best-effort)
        tasks = []
        if self._task_retransmit:
            self._task_retransmit.cancel()
            tasks.append(self._task_retransmit)
            self._task_retransmit = None
        if self._task_cleanup:
            self._task_cleanup.cancel()
            tasks.append(self._task_cleanup)
            self._task_cleanup = None

        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*[asyncio.shield(t) for t in tasks], return_exceptions=True), timeout=wait_timeout)
            except Exception:
                # ignore timeouts / cancelled errors, we are forcing shutdown
                pass

        # Close transport
        try:
            if self.transport:
                try:
                    self.transport.close()
                except Exception:
                    logger.exception("Error closing ML transport")
                self.transport = None
        except Exception:
            logger.exception("Error during transport close")

        # Clean internal maps (best-effort)
        try:
            with self._lock:
                self.pending_outgoing.clear()
                self.last_acks.clear()
                self.seen_msgs.clear()
        except Exception:
            logger.exception("Error clearing internal ML server state")

        logger.info("MLServerProtocol stopped")

    def stop_sync(self, wait_timeout: float = 5.0) -> None:
        """
        Synchronous wrapper to stop the ML server from another thread.
        Uses run_coroutine_threadsafe on the loop that owns the protocol (if available).
        """
        if self._loop is None:
            # nothing scheduled; nothing to stop
            try:
                if self.transport:
                    self.transport.close()
            except Exception:
                pass
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(self.stop(wait_timeout), self._loop)
            try:
                fut.result(timeout=wait_timeout + 1.0)
            except Exception:
                fut.cancel()
        except Exception:
            logger.exception("stop_sync failed; attempted best-effort transport close")
            try:
                if self.transport:
                    self.transport.close()
            except Exception:
                pass

# -------------------------------------------------------------------
# NOVO CÓDIGO DE EXECUÇÃO (main)
# -------------------------------------------------------------------

async def _run_ml_server_main(server_protocol: MLServerProtocol, store: MissionStore):
    """
    Função assíncrona que inicia e mantém o servidor ML a correr.
    """
    loop = asyncio.get_running_loop()
    
    # 1. Cria missões demo para garantir que há algo para atribuir
    store.create_demo_missions()
    logger.info("Created demo missions in MissionStore.")

    # 2. Inicia o servidor UDP
    # O create_datagram_endpoint irá chamar connection_made no server_protocol
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: server_protocol,
        local_addr=('0.0.0.0', config.ML_UDP_PORT),
        family=socket.AF_INET
    )
    
    # Mantém o servidor a correr até ser cancelado (e.g. por KeyboardInterrupt)
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        # Garante a paragem em caso de cancelamento
        await server_protocol.stop()


def main():
    """Entry point for the MissionLink Server."""
    try:
        from common import config
        # Garantir que o logging está configurado (já feito pelo if __name__ == "__main__")

        # 1. Inicializa o MissionStore (que carrega/cria missões)
        store = MissionStore(persist_file=config.MISSION_STORE_FILE)
        
        # 2. Inicializa o Protocolo ML
        protocol = MLServerProtocol(mission_store=store)
        
        # 3. Executa o loop assíncrono
        asyncio.run(_run_ml_server_main(protocol, store))

    except KeyboardInterrupt:
        logger.info("MLServer exiting (via KeyboardInterrupt)")
    except Exception:
        logger.exception("Unexpected error in MLServer main loop")


if __name__ == "__main__":
    # Garantir que o logging está configurado
    import logging
    try:
        from common import config
        config.configure_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
        
    main()