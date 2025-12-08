#!/usr/bin/env python3
"""
Rover agent que integra RoverSim + ML missionLink client + (opcional) criação de missões via API
e envio de telemetria TCP.

Nota: vem com correção importante — ao receber MISSION_ASSIGN o agente envia
um ML_ACK confirmando o msgid do servidor. Isto evita retransmits e undeliverable_assign.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import socket
import struct
import time
from typing import Any, Dict, Optional, Tuple, List

from common import binary_proto, config, utils
from rover.rover_sim import RoverSim

logger = utils.get_logger("rover.agent")

# Only import requests when needed (so agent can run without requests if not creating missions)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


DEFAULT_TASKS = ["capture_images", "collect_samples", "env_analysis"]


class RoverAgent(asyncio.DatagramProtocol):
    def __init__(
        self,
        rover_id: str,
        server: Tuple[str, int],
        interval: float = 1.0,
        exit_on_complete: bool = False,
        request_interval: float = 5.0,
        telemetry_enabled: bool = False,
        telemetry_addr: Tuple[str, int] = ("127.0.0.1", config.TELEMETRY_PORT),
    ):
        self.rover_id = rover_id
        self.server = server
        self.interval = interval
        self.exit_on_complete = exit_on_complete
        self.request_interval = request_interval

        self.transport = None
        self.rover = RoverSim(rover_id)
        self._progress_task: Optional[asyncio.Task] = None
        self._request_task: Optional[asyncio.Task] = None
        self._assigned_mission_id: Optional[str] = None

        self.seq = random.getrandbits(16)

        # telemetry TCP
        self.telemetry_enabled = telemetry_enabled
        self.telemetry_addr = telemetry_addr
        self._telemetry_writer: Optional[asyncio.StreamWriter] = None

    def connection_made(self, transport):
        self.transport = transport
        try:
            sockname = transport.get_extra_info("sockname")
            logger.info("UDP socket bound at %s", sockname)
        except Exception:
            logger.debug("Could not get transport sockname")
        loop = asyncio.get_event_loop()
        # start request loop and (lazy) telemetry connection
        self._request_task = loop.create_task(self._request_loop())
        if self.telemetry_enabled:
            loop.create_task(self._ensure_telemetry_connection())

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

        # Extract server msgid (numeric) if present
        server_msgid = None
        try:
            server_msgid = int(header.get("msgid")) if header.get("msgid") is not None else None
        except Exception:
            server_msgid = None

        if mtype == binary_proto.ML_MISSION_ASSIGN:
            mission_id = canonical.get("mission_id")
            params = {}
            pj = canonical.get("params")
            if pj:
                try:
                    params = json.loads(pj)
                except Exception:
                    try:
                        params = json.loads(pj.decode("utf-8") if isinstance(pj, (bytes, bytearray)) else str(pj))
                    except Exception:
                        params = {}
            logger.info("Received MISSION_ASSIGN %s params=%s", mission_id, params)

            # --- SEND ACK for the incoming MISSION_ASSIGN so server knows it was delivered ---
            try:
                ack_val = int(server_msgid or 0) & 0xFFFFFFFFFFFFFFFF
                ack_tlv = (binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", ack_val))
                ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, self.rover_id, [ack_tlv], msgid=random.getrandbits(48))
                if self.transport:
                    try:
                        self.transport.sendto(ack_pkt, addr)
                        logger.debug("Sent ACK for assign msgid=%s to %s", server_msgid, addr)
                    except Exception:
                        logger.exception("Failed to send ACK for assign to %s", addr)
            except Exception:
                logger.exception("Error while building/sending ACK for MISSION_ASSIGN")

            # --------------------------------------------------------------------------

            mission_spec = {"mission_id": mission_id, "task": params.get("task", ""), "params": params}
            try:
                self._assigned_mission_id = mission_id
                if self._request_task and not self._request_task.done():
                    self._request_task.cancel()
                    self._request_task = None
                self.rover.start_mission(mission_spec)
                if self._progress_task is None or self._progress_task.done():
                    self._progress_task = asyncio.get_event_loop().create_task(self._progress_loop())
            except Exception:
                logger.exception("Failed to start mission in RoverSim")

        elif mtype == binary_proto.ML_ACK:
            tlv_ack = tlvmap.get(binary_proto.TLV_ACKED_MSG_ID, [])
            if tlv_ack:
                try:
                    acked = struct.unpack(">Q", tlv_ack[0])[0]
                    logger.info("Received ACK for outgoing msg %s", acked)
                except Exception:
                    pass
        elif mtype == binary_proto.ML_ERROR:
            logger.warning("ML ERROR: %s", binary_proto.tlv_to_canonical(tlvmap).get("errors"))
        else:
            logger.debug("Unhandled ML msgtype %s", mtype)

    async def _send_request_once(self):
        self.seq = (self.seq + 1) & 0xFFFFFFFF
        msgid = random.getrandbits(48)
        tlvs = []
        caps = ",".join(["sampling", "imaging", "env"])
        tlvs.append((binary_proto.TLV_CAPABILITIES, caps.encode("utf-8")))
        pkt = binary_proto.pack_ml_datagram(
            binary_proto.ML_REQUEST_MISSION,
            self.rover_id,
            tlvs,
            flags=binary_proto.FLAG_ACK_REQUESTED,
            seqnum=self.seq,
            msgid=msgid,
        )
        try:
            if self.transport:
                self.transport.sendto(pkt, self.server)
                logger.info("Sent REQUEST_MISSION msgid=%s to %s", msgid, self.server)
        except Exception:
            logger.exception("Failed to send REQUEST_MISSION")

    async def _request_loop(self):
        """Repeat REQUEST_MISSION until we receive an ASSIGN."""
        logger.info("Starting request loop (interval=%ss) to poll for missions", self.request_interval)
        try:
            while self._assigned_mission_id is None:
                await self._send_request_once()
                await asyncio.sleep(self.request_interval)
        except asyncio.CancelledError:
            logger.debug("_request_loop cancelled")
        except Exception:
            logger.exception("Error in _request_loop")

    async def _progress_loop(self):
        logger.info("Starting progress loop for mission %s (interval=%s)", self._assigned_mission_id, self.interval)
        while True:
            await asyncio.sleep(self.interval)
            try:
                self.rover.step(self.interval)
            except Exception:
                logger.exception("RoverSim step failed")

            tel = self.rover.get_telemetry()
            mission_id = tel.get("mission_id") or self._assigned_mission_id
            progress = float(tel.get("progress_pct", 0.0) or 0.0)

            tlvs = []
            if mission_id:
                tlvs.append((binary_proto.TLV_MISSION_ID, str(mission_id).encode("utf-8")))
            tlvs.append(binary_proto.tlv_progress(progress))
            pos = tel.get("position", None)
            if pos:
                tlvs.append(
                    binary_proto.tlv_position(float(pos.get("x", 0.0)), float(pos.get("y", 0.0)), float(pos.get("z", 0.0)))
                )
            batt = tel.get("battery_level_pct")
            if batt is not None:
                tlvs.append(binary_proto.tlv_battery_level(int(batt)))
            if "internal_temp_c" in tel:
                params = {"internal_temp_c": tel["internal_temp_c"], "status": tel.get("status"), "errors": tel.get("errors", [])}
                tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(params).encode("utf-8")))

            msgid = random.getrandbits(48)
            pkt = binary_proto.pack_ml_datagram(
                binary_proto.ML_PROGRESS,
                self.rover_id,
                tlvs,
                flags=binary_proto.FLAG_ACK_REQUESTED,
                seqnum=self.seq,
                msgid=msgid,
            )
            try:
                if self.transport:
                    self.transport.sendto(pkt, self.server)
                    logger.info("Sent PROGRESS %s pct=%.1f", mission_id, progress)
            except Exception:
                logger.exception("Failed to send PROGRESS")

            # Note: telemetry JSON over raw TCP is NOT compatible with the TelemetryServer framing
            # in this project. If you use --telemetry you'll get frame/connection errors.
            # For now we don't send telemetry JSON by default; enable only when implementing the
            # server-side framing or a proper HTTP telemetry ingestion endpoint.

            # Completion condition
            if self.rover.is_mission_complete() and (self.rover.state == "COMPLETED" or self.rover.state in ("CHARGING_TRAVEL", "CHARGING")):
                tlvs = []
                if mission_id:
                    tlvs.append((binary_proto.TLV_MISSION_ID, str(mission_id).encode("utf-8")))
                result = {"result": "success", "samples_collected": self.rover.samples_collected}
                tlvs.append((binary_proto.TLV_PARAMS_JSON, json.dumps(result).encode("utf-8")))
                complete_msgid = random.getrandbits(48)
                complete_pkt = binary_proto.pack_ml_datagram(
                    binary_proto.ML_MISSION_COMPLETE,
                    self.rover_id,
                    tlvs,
                    flags=binary_proto.FLAG_ACK_REQUESTED,
                    seqnum=self.seq,
                    msgid=complete_msgid,
                )
                try:
                    if self.transport:
                        self.transport.sendto(complete_pkt, self.server)
                        logger.info("Sent MISSION_COMPLETE %s", mission_id)
                except Exception:
                    logger.exception("Failed to send MISSION_COMPLETE")

                if self.exit_on_complete:
                    logger.info("Mission complete and exit_on_complete set - stopping agent")
                    try:
                        if self.transport:
                            self.transport.close()
                    except Exception:
                        pass
                    asyncio.get_event_loop().stop()
                    return
                else:
                    self._assigned_mission_id = None
                    logger.info("Mission finished; restarting request loop for new missions")
                    if self._request_task is None or self._request_task.done():
                        self._request_task = asyncio.get_event_loop().create_task(self._request_loop())
                    return

    def error_received(self, exc):
        logger.exception("Socket error: %s", exc)

    def connection_lost(self, exc):
        logger.info("UDP connection lost")
        if self._progress_task and not self._progress_task.done():
            self._progress_task.cancel()
        if self._request_task and not self._request_task.done():
            self._request_task.cancel()
        if self._telemetry_writer:
            try:
                self._telemetry_writer.close()
            except Exception:
                pass


# ----------------------
# Helpers to create missions via API (optional, robust)
# ----------------------
def _http_ok(code: int) -> bool:
    return code in (200, 201, 202, 204)


def _try_post_create(api: str, payload: Dict[str, Any], timeout: float = 5.0) -> Tuple[bool, Optional[int], Optional[str]]:
    if requests is None:
        return False, None, "requests-not-available"
    try:
        r = requests.post(f"{api}/api/missions", json=payload, timeout=timeout)
        return _http_ok(r.status_code), r.status_code, r.text
    except Exception as e:
        return False, None, str(e)


def _try_put_create(api: str, mission_id: str, payload: Dict[str, Any], timeout: float = 5.0) -> Tuple[bool, Optional[int], Optional[str]]:
    if requests is None:
        return False, None, "requests-not-available"
    try:
        r = requests.put(f"{api}/api/missions/{mission_id}", json=payload, timeout=timeout)
        return _http_ok(r.status_code), r.status_code, r.text
    except Exception as e:
        return False, None, str(e)


def _try_assign(api: str, mission_id: str, rover_id: str, timeout: float = 5.0) -> Tuple[bool, Optional[int], Optional[str]]:
    if requests is None:
        return False, None, "requests-not-available"
    api = api.rstrip("/")
    try:
        r = requests.post(f"{api}/api/missions/{mission_id}/assign", json={"rover_id": rover_id}, timeout=timeout)
        if _http_ok(r.status_code):
            return True, r.status_code, r.text
    except Exception:
        pass
    try:
        r2 = requests.patch(f"{api}/api/missions/{mission_id}", json={"assigned_rover": rover_id, "state": "ASSIGNED"}, timeout=timeout)
        if _http_ok(r2.status_code):
            return True, r2.status_code, r2.text
        return False, r2.status_code, r2.text
    except Exception as e:
        return False, None, str(e)


def create_missions_via_api(api_base: str, count: int, base_id: str = "M-AUTO", task: Optional[str] = None, params: Optional[Dict[str, Any]] = None, priority: int = 1, assign_to: Optional[str] = None) -> List[str]:
    created = []
    if requests is None:
        logger.error("requests library not available; cannot create missions via API")
        return created

    api = api_base.rstrip("/")
    for i in range(count):
        mid = f"{base_id}-{int(time.time())%100000}-{i}"
        t = task or DEFAULT_TASKS[i % len(DEFAULT_TASKS)]
        payload = {"mission_id": mid, "task": t, "params": params or {}, "priority": priority}
        ok, status, text = _try_post_create(api, payload)
        if ok:
            logger.info("Created mission %s via POST -> %s", mid, status)
            created.append(mid)
        else:
            logger.warning("POST create %s failed (status=%s text=%s), trying PUT fallback", mid, status, text)
            ok2, status2, text2 = _try_put_create(api, mid, payload)
            if ok2:
                logger.info("Created mission %s via PUT -> %s", mid, status2)
                created.append(mid)
            else:
                logger.error("Failed to create mission %s (POST status=%s text=%s; PUT status=%s text=%s)", mid, status, text, status2, text2)
                continue

        time.sleep(0.05)
        if assign_to:
            assigned, st, tx = _try_assign(api, mid, assign_to)
            if assigned:
                logger.info("Assigned mission %s -> %s (status=%s)", mid, assign_to, st)
            else:
                logger.warning("Assign failed for %s -> %s (status=%s text=%s)", mid, assign_to, st, tx)

    return created


# ----------------------
# Entrypoint
# ----------------------
async def main(
    rover_id: str,
    server_host: str,
    server_port: int,
    interval: float,
    exit_on_complete: bool,
    request_interval: float,
    create_missions: int,
    assign_created: bool,
    api_base: str,
    telemetry_enabled: bool,
    telemetry_host: str,
    telemetry_port: int,
):
    loop = asyncio.get_running_loop()

    if create_missions and create_missions > 0:
        logger.info("Creating %s missions via API %s (assign_created=%s)", create_missions, api_base, assign_created)
        created = create_missions_via_api(api_base, create_missions, base_id="M-AUTO", assign_to=(rover_id if assign_created else None))
        logger.info("Created missions: %s", created)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception:
        pass
    sock.bind(("0.0.0.0", 0))

    agent = RoverAgent(
        rover_id,
        (server_host, server_port),
        interval=interval,
        exit_on_complete=exit_on_complete,
        request_interval=request_interval,
        telemetry_enabled=telemetry_enabled,
        telemetry_addr=(telemetry_host, telemetry_port),
    )

    transport, protocol = await loop.create_datagram_endpoint(lambda: agent, sock=sock)
    logger.info("RoverAgent started (rover=%s server=%s:%d interval=%s request_interval=%s telemetry=%s)", rover_id, server_host, server_port, interval, request_interval, telemetry_enabled)

    stop = asyncio.Event()
    try:
        await stop.wait()
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rover-id", required=True)
    p.add_argument("--server", default="127.0.0.1")
    p.add_argument("--port", type=int, default=config.ML_UDP_PORT)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--request-interval", type=float, default=5.0)
    p.add_argument("--exit-on-complete", action="store_true")
    p.add_argument("--create-missions", type=int, default=0, help="If >0, create this many missions via Core API before polling")
    p.add_argument("--assign-created", action="store_true", help="Assign created missions immediately to this rover (useful for tests)")
    p.add_argument("--api", dest="api_base", default="http://127.0.0.1:65000", help="Core API base URL")
    p.add_argument("--telemetry", dest="telemetry_enabled", action="store_true", help="Also send telemetry JSON to TelemetryServer TCP (NOT enabled by default in repro)")
    p.add_argument("--telemetry-host", default="127.0.0.1")
    p.add_argument("--telemetry-port", type=int, default=config.TELEMETRY_PORT)
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    try:
        asyncio.run(
            main(
                args.rover_id,
                args.server,
                args.port,
                args.interval,
                args.exit_on_complete,
                args.request_interval,
                args.create_missions,
                args.assign_created,
                args.api_base,
                args.telemetry_enabled,
                args.telemetry_host,
                args.telemetry_port,
            )
        )
    except KeyboardInterrupt:
        pass