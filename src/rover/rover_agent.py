from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import socket
import struct
import time
import threading
from typing import Any, Dict, Optional, Tuple, List

from common import binary_proto, config, utils, mission_schema
from rover.rover_sim import RoverSim

logger = utils.get_logger("rover.agent")


DEFAULT_TASKS = ["capture_images", "collect_samples", "env_analysis"]


class RoverAgent(asyncio.DatagramProtocol):
    def __init__(
        self,
        rover_id: str,
        server: Tuple[str, int],
        request_interval: float = 5.0, 
    ):
        self.rover_id = rover_id
        self.server = server
        self.request_interval = request_interval

        self.transport = None
        self.rover = RoverSim(rover_id)
        self._progress_task: Optional[asyncio.Task] = None
        self._request_task: Optional[asyncio.Task] = None
        self._assigned_mission_id: Optional[str] = None
        self._interrupted_mission_id: Optional[str] = None 
        self._progress_interval: float = 1.0 

        self.seq = random.getrandbits(16)
        self.loop = None
        
        self.pending: Dict[int, Tuple[bytes, float, int]] = {}
        self._lock = threading.Lock()
        self._task_retransmit: Optional[asyncio.Task] = None
        self._last_acks: Dict[int, bytes] = {} 

    def connection_made(self, transport):
        self.transport = transport
        try:
            sockname = transport.get_extra_info("sockname")
            logger.info("UDP socket bound at %s", sockname)
        except Exception:
            logger.debug("Could not get transport sockname")
        
        self.loop = asyncio.get_event_loop()
        
        self._task_retransmit = self.loop.create_task(self._retransmit_loop())
        
        self._request_task = self.loop.create_task(self._request_loop())

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        try:
            parsed = binary_proto.parse_ml_datagram(data)
        except Exception as e:
            logger.warning("Failed to parse ML datagram from %s: %s", addr, e)
            return

        header = parsed.get("header", {})
        mtype = header.get("msgtype")
        tlvmap = parsed.get("tlvs", {})
        canonical = binary_proto.tlv_to_canonical(tlvmap)

        server_msgid = None
        try:
            server_msgid = int(header.get("msgid")) if header.get("msgid") is not None else None
        except Exception:
            server_msgid = None

        if mtype == binary_proto.ML_ACK:
            tlv_ack = tlvmap.get(binary_proto.TLV_ACKED_MSG_ID, [])
            if tlv_ack:
                try:
                    acked = struct.unpack(">Q", tlv_ack[0])[0]
                    with self._lock:
                        if acked in self.pending:
                            self.pending.pop(acked, None)
                            logger.info("Received ACK for outgoing msg %s", acked)
                except Exception:
                    pass
            return

        if mtype == binary_proto.ML_MISSION_ASSIGN:
            
            mission_id = canonical.get("mission_id")
            
            if self._assigned_mission_id and self._assigned_mission_id != mission_id:
                logger.warning("Received MISSION_ASSIGN for %s while already running mission %s. Ignoring new assignment.", mission_id, self._assigned_mission_id)
                self.send_ack(server_msgid, addr) 
                return
            
            if self._assigned_mission_id == mission_id and not self._interrupted_mission_id:
                 logger.info("Received re-ASSIGN for already running mission %s. Likely server retransmit. Acknowledge and ignore.", mission_id)
                 self.send_ack(server_msgid, addr)
                 return
                 
            mission_spec = mission_schema.mission_spec_from_tlvmap(tlvmap)
            
            logger.info("Received MISSION_ASSIGN %s task=%s", mission_id, mission_spec.get('task'))

            self.send_ack(server_msgid, addr)

            try:
                self._assigned_mission_id = mission_id
                self._interrupted_mission_id = None 
                
                if self._request_task and not self._request_task.done():
                    self._request_task.cancel()
                    self._request_task = None
                
                self._progress_interval = mission_spec.get("params", {}).get("update_interval_s", 1.0)
                
                self.rover.start_mission(mission_spec)
                
                if self._progress_task is None or self._progress_task.done():
                    self._progress_task = self.loop.create_task(self._progress_loop())
            except Exception:
                logger.exception("Failed to start mission in RoverSim")

        elif mtype == binary_proto.ML_ERROR:
            logger.warning("ML ERROR: %s", binary_proto.tlv_to_canonical(tlvmap).get("errors"))
        else:
            logger.debug("Unhandled ML msgtype %s", mtype)

    def send_ack(self, acked_msgid: int, addr: Tuple[str, int]) -> None:
        """Envia ACK para uma mensagem recebida (pode ser chamada de forma síncrona)."""
        if acked_msgid is None or not self.transport:
            return
        
        ack_val = int(acked_msgid) & 0xFFFFFFFFFFFFFFFF
        ack_tlv = (binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", ack_val))
        ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, self.rover_id, [ack_tlv], msgid=random.getrandbits(48))

        try:
            self.transport.sendto(ack_pkt, addr)
            logger.debug("Sent ACK for msgid=%s to %s", acked_msgid, addr)
        except Exception:
            logger.exception("Failed to send ACK to %s", addr)

    async def _send_ml_packet(self, msg_type: int, tlvs: List[Tuple[int, bytes]], flags: int = 0) -> int:
        """Helper para criar e enviar um pacote ML com fiabilidade."""
        self.seq = (self.seq + 1) & 0xFFFFFFFF
        msgid = random.getrandbits(48)
        
        pkt = binary_proto.pack_ml_datagram(
            msg_type,
            self.rover_id,
            tlvs,
            flags=flags | binary_proto.FLAG_ACK_REQUESTED, 
            seqnum=self.seq,
            msgid=msgid,
        )
        
        if len(pkt) > getattr(config, "ML_MAX_DATAGRAM_SIZE", 1200):
            logger.error("Packet too large (%d bytes) - not sent", len(pkt))
            return 0
            
        with self._lock:
            self.pending[int(msgid)] = (pkt, time.time(), 1)
            
        try:
            self.transport.sendto(pkt, self.server)
            return msgid
        except Exception:
            logger.exception("Failed to send ML packet")
            with self._lock:
                 self.pending.pop(int(msgid), None)
            return 0


    async def request_mission(self, resume_id: Optional[str] = None): 
        """Envia REQUEST_MISSION para o servidor, com a opção de pedir a retoma de uma missão."""
        tlvs = []
        caps = ",".join(["sampling", "imaging", "env"])
        tlvs.append((binary_proto.TLV_CAPABILITIES, caps.encode("utf-8")))
        
        if resume_id:
            tlvs.append((binary_proto.TLV_MISSION_ID, str(resume_id).encode("utf-8")))
        
        msgid = await self._send_ml_packet(binary_proto.ML_REQUEST_MISSION, tlvs)
        
        if msgid:
            log_msg = f"Sent REQUEST_MISSION (Resume: {resume_id}) msgid={msgid} to {self.server}"
            logger.info(log_msg)

    async def _send_maintenance_progress(self):
        """Envia um PROGRESS de status de manutenção (sem mission_id)."""
        tel = self.rover.get_telemetry()
        tlvs: List[Tuple[int, bytes]] = []
        
        batt = tel.get("battery_level_pct")
        if batt is not None:
            tlvs.append(binary_proto.tlv_battery_level(int(batt)))
            
        pos = tel.get("position", None)
        if pos:
            tlvs.append(binary_proto.tlv_position(float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0))))

        custom_params = {
            "status": tel.get("status"),
            "battery_level_pct": tel.get("battery_level_pct"),
            "internal_temp_c": tel.get("internal_temp_c"),
            "current_speed_m_s": tel.get("current_speed_m_s"),
        }
        tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(custom_params).encode("utf-8")))

        msgid = await self._send_ml_packet(binary_proto.ML_PROGRESS, tlvs)
        
        if msgid:
            logger.info("Sent MAINTENANCE PROGRESS (Status: %s | Batt: %.1f%% | Pos: %.1f,%.1f)", 
                        tel.get("status", "N/A").upper(), batt, pos["x"], pos["y"])

    async def _request_loop(self):
        """Repete REQUEST_MISSION até receber um ASSIGN, e gere os estados de manutenção (CHARGING/COOLING/TRAVEL)."""
        logger.info("Starting request/maintenance loop (interval=%.1fs) to poll for missions", self.request_interval)
        try:
            while self._assigned_mission_id is None:
                
                try:
                    self.rover.step(self.request_interval) 
                except Exception:
                    logger.exception("RoverSim step failed in maintenance loop")
                
                new_state = self.rover.state.upper()
                
                if new_state in ("IDLE", "COMPLETED", "ERROR"):
                    
                    mission_to_request = self._interrupted_mission_id 
                    
                    if mission_to_request:
                        logger.info("Rover is ready to resume mission %s.", mission_to_request)
                        await self.request_mission(mission_to_request) 
                        self._interrupted_mission_id = None 
                        
                    else:
                        await self.request_mission()
                
                elif new_state in ("CHARGING_TRAVEL", "CHARGING", "COOLING"):
                    await self._send_maintenance_progress()
                
                await asyncio.sleep(self.request_interval)

        except asyncio.CancelledError:
            logger.debug("_request_loop cancelled")
        except Exception:
            logger.exception("Error in _request_loop")


    async def _progress_loop(self):
        """Executa a simulação (step) e envia PROGRESS (ML) na frequência da missão."""
        mission_id = self._assigned_mission_id
        interval = self._progress_interval
        logger.info("Starting progress loop for mission %s (interval=%.1fs)", mission_id, interval)
        
        while mission_id == self._assigned_mission_id:
            await asyncio.sleep(interval)
            
            try:
                self.rover.step(interval)
            except Exception:
                logger.exception("RoverSim step failed")

            tel = self.rover.get_telemetry()
            current_state = self.rover.state.upper() 
            
            tlvs: List[Tuple[int, bytes]] = []
            if mission_id:
                tlvs.append((binary_proto.TLV_MISSION_ID, str(mission_id).encode("utf-8")))
            
            progress = tel.get("progress_pct", 0.0) 
            pos = tel.get("position", None)
            batt = tel.get("battery_level_pct")

            if progress is not None:
                tlvs.append(binary_proto.tlv_progress(float(progress)))
            if pos:
                tlvs.append(binary_proto.tlv_position(float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0))))
            if batt is not None:
                tlvs.append(binary_proto.tlv_battery_level(int(batt)))
            
            custom_params = {
                "internal_temp_c": tel.get("internal_temp_c"),
                "current_speed_m_s": tel.get("current_speed_m_s"),
                "status": tel.get("status"),
                "errors": tel.get("errors", []),
                "position": tel.get("position"),
                "battery_level_pct": tel.get("battery_level_pct")
            }
            tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(custom_params).encode("utf-8")))

            msgid = await self._send_ml_packet(binary_proto.ML_PROGRESS, tlvs)
            
            if msgid:
                 status_log = self.rover.state.upper()
                 temp_log = tel.get("internal_temp_c", 0.0)
                 logger.info("Sent PROGRESS %s pct=%.1f (Status: %s | Temp: %.1f)", 
                             mission_id, float(progress), status_log, temp_log)


            if current_state in ("CHARGING_TRAVEL", "CHARGING", "COOLING", "ERROR"):
                logger.warning("Mission %s paused/interrupted (State: %s). Stopping progress loop. Registering ID for resumption.", mission_id, current_state)
                
                self._interrupted_mission_id = mission_id
                self._assigned_mission_id = None
                self._progress_interval = 1.0 
                
                if self._request_task is None or self._request_task.done():
                    self._request_task = self.loop.create_task(self._request_loop())
                return 

            if self.rover.is_mission_complete():
                
                tlvs: List[Tuple[int, bytes]] = []
                if mission_id:
                    tlvs.append((binary_proto.TLV_MISSION_ID, str(mission_id).encode("utf-8")))
                result = {"result": "success", "samples_collected": self.rover.samples_collected}
                tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(result).encode("utf-8")))
                
                await self._send_ml_packet(binary_proto.ML_MISSION_COMPLETE, tlvs)
                logger.info("Sent MISSION_COMPLETE %s (State: %s)", mission_id, self.rover.state)

                self._assigned_mission_id = None
                self._interrupted_mission_id = None 
                self._progress_interval = 1.0 
                
                if self._request_task is None or self._request_task.done():
                    self._request_task = self.loop.create_task(self._request_loop())
                return


    async def _retransmit_loop(self):
        """Loop de fundo para retransmissão de pacotes pendentes (garantia ML)."""
        while True:
            now = time.time()
            to_remove = []
            with self._lock:
                items = list(self.pending.items())
            for msg_id, (pkt, created, attempts) in items:
                timeout = config.TIMEOUT_TX_INITIAL * (config.BACKOFF_FACTOR ** (attempts - 1))
                if now - created > timeout:
                    if attempts <= config.N_RETX:
                        try:
                            self.transport.sendto(pkt, self.server)
                            with self._lock:
                                self.pending[msg_id] = (pkt, now, attempts + 1)
                            logger.warning("Retransmit pending msg %s attempt=%d", msg_id, attempts + 1)
                        except Exception:
                            logger.exception("Failed retransmit")
                    else:
                        logger.error("Retries exhausted for pending msg %s", msg_id)
                        to_remove.append(msg_id)
            with self._lock:
                for r in to_remove:
                    self.pending.pop(r, None)
            await asyncio.sleep(0.5)


    def connection_lost(self, exc):
        logger.info("UDP connection lost")
        if self._progress_task and not self._progress_task.done():
            self._progress_task.cancel()
        if self._request_task and not self._request_task.done():
            self._request_task.cancel()
        if self._task_retransmit and not self._task_retransmit.done():
            self._task_retransmit.cancel()
            
            
async def run_agent_main(
    rover_id: str,
    server_host: str,
    server_port: int,
    request_interval: float,
):
    loop = asyncio.get_running_loop()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception:
        pass
    sock.bind(("0.0.0.0", 0))

    agent = RoverAgent(
        rover_id,
        (server_host, server_port),
        request_interval=request_interval,
    )

    transport, protocol = await loop.create_datagram_endpoint(lambda: agent, sock=sock)
    logger.info("RoverAgent started (rover=%s server=%s:%d request_interval=%.1fs)", 
                rover_id, server_host, server_port, request_interval)

    stop = asyncio.Event()
    try:
        await stop.wait()
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rover-id", required=True)
    p.add_argument("--server", default="10.0.10.1")
    p.add_argument("--port", type=int, default=config.ML_UDP_PORT)
    p.add_argument("--request-interval", type=float, default=5.0)
    args = p.parse_args()

    try:
        asyncio.run(
            run_agent_main(
                args.rover_id,
                args.server,
                args.port,
                args.request_interval,
            )
        )
    except KeyboardInterrupt:
        logger.info("Agent shutting down via KeyboardInterrupt")
    except Exception:
        logger.exception("Unexpected error in agent main loop")


if __name__ == "__main__":
    from common import config
    config.configure_logging()
    main()