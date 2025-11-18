#!/usr/bin/env python3
"""
Simple ML UDP client (binary) with retransmit, persisted-packet resend and dedupe support.

Features:
- persistent cache directory support via ML_CLIENT_CACHE_DIR env var
- _resend_persisted_packets(addr) to read files "<rover_id>-<msgid>.bin" and resend them
- on receiving ACK for a msgid, remove the corresponding persisted file if present
- dedupe for incoming messages: if same msgid arrives again, resend the previous ACK bytes
"""
from __future__ import annotations

import asyncio
import socket
import time
import argparse
import logging
import struct
import random
import threading
import os
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from common import binary_proto, config, utils

logger = utils.get_logger("rover.ml_client")


class SimpleMLClient(asyncio.DatagramProtocol):
    def __init__(self, rover_id: str, server: Tuple[str, int] = ("127.0.0.1", config.ML_UDP_PORT), exit_on_complete: bool = False):
        self.rover_id = rover_id
        self.server = server
        self.exit_on_complete = exit_on_complete
        self.transport = None
        # pending: msgid -> (packet, created_at, attempts)
        self.pending: Dict[int, Tuple[bytes, float, int]] = {}
        # dedupe: seen incoming msgids -> timestamp
        self.seen_msgs: Dict[int, float] = {}
        # last ACKs sent for incoming msgid -> bytes
        self.last_acks: Dict[int, bytes] = {}
        self.seen_acks = set()
        # avoid calling get_event_loop() at init time when no loop is running
        try:
            # get_running_loop() raises RuntimeError if no loop is running in this thread
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = None
        self.seq = random.getrandbits(16)
        self._lock = threading.Lock()
        # persistent cache dir and map msgid -> Path for persisted packets
        cache_dir = os.environ.get("ML_CLIENT_CACHE_DIR", "")
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self._persisted_files: Dict[int, Path] = {}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        try:
            parsed = binary_proto.parse_ml_datagram(data)
        except Exception as e:
            logger.warning("Failed parse ML datagram from %s: %s", addr, e)
            return
        header = parsed.get("header", {})
        code = header.get("msgtype")
        tlvs = parsed.get("tlvs", {})
        canonical = binary_proto.tlv_to_canonical(tlvs)

        # Extract numeric incoming msgid for dedupe behavior
        try:
            incoming_msgid = int(header.get("msgid") or 0)
        except Exception:
            incoming_msgid = 0

        # If duplicate: resend last ACK if available and skip processing
        if incoming_msgid and incoming_msgid in self.seen_msgs:
            last = self.last_acks.get(incoming_msgid)
            if last and self.transport:
                try:
                    self.transport.sendto(last, addr)
                    logger.debug("Resent last ACK for duplicate incoming msg %s to %s", incoming_msgid, addr)
                except Exception:
                    logger.exception("Failed to resend last ACK for duplicate incoming msg %s", incoming_msgid)
            else:
                logger.debug("Duplicate incoming msg %s but no stored ACK to resend", incoming_msgid)
            return

        # mark seen
        if incoming_msgid:
            self.seen_msgs[incoming_msgid] = time.time()

        # handle ACKs: TLV_ACKED_MSG_ID (server acknowledging our outgoing messages)
        acked_bytes = tlvs.get(binary_proto.TLV_ACKED_MSG_ID, [])
        if acked_bytes:
            try:
                acked = struct.unpack(">Q", acked_bytes[0])[0]
                with self._lock:
                    if acked in self.pending:
                        self.pending.pop(acked, None)
                        logger.info("Received ACK for %s", acked)
                    # remove persisted file if present
                    pf = self._persisted_files.pop(int(acked), None)
                if pf:
                    try:
                        if pf.exists():
                            pf.unlink()
                            logger.info("Removed persisted packet file %s after ACK %s", pf, acked)
                    except Exception:
                        logger.exception("Failed to remove persisted packet file %s", pf)

            except Exception:
                logger.exception("Failed to unpack ACKed msgid")

        # mission assign handling - send ACK back (keep behavior)
        if code == binary_proto.ML_MISSION_ASSIGN:
            mission_id = canonical.get("mission_id")
            logger.info("Received MISSION_ASSIGN %s", mission_id)

            try:
                server_msgid = None
                if header and "msgid" in header and header["msgid"] is not None:
                    try:
                        server_msgid = int(header["msgid"])
                    except Exception:
                        server_msgid = None
                if server_msgid is None:
                    server_msgid = 0

                ack_tlv = (binary_proto.TLV_ACKED_MSG_ID, struct.pack(">Q", int(server_msgid) & 0xFFFFFFFFFFFFFFFF))
                ack_pkt = binary_proto.pack_ml_datagram(binary_proto.ML_ACK, self.rover_id, [ack_tlv], msgid=random.getrandbits(48))

                # send ACK and store it keyed by incoming_msgid so duplicates can reuse same bytes
                if self.transport:
                    try:
                        self.transport.sendto(ack_pkt, addr)
                        logger.debug("Sent ACK for assign msgid=%s to %s", server_msgid, addr)
                    except Exception:
                        logger.exception("Failed to send ACK for assign")
                # store copy of the ACK to allow exact re-send on duplicates
                if incoming_msgid:
                    self.last_acks[incoming_msgid] = ack_pkt

            except Exception:
                logger.exception("Error while sending ACK for MISSION_ASSIGN")

            # schedule handling / progress loop (preserve previous behavior)
            if self.loop:
                # schedule the progress loop on the running loop
                asyncio.run_coroutine_threadsafe(self._progress_loop(mission_id), self.loop)
            else:
                # if no loop available (e.g. in unit tests), start progress loop in a new thread/event loop
                def _start_loop_in_thread():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self._progress_loop(mission_id))
                    finally:
                        loop.close()
                thr = threading.Thread(target=_start_loop_in_thread, daemon=True)
                thr.start()

    def error_received(self, exc):
        logger.exception("Socket error: %s", exc)

    def connection_lost(self, exc):
        logger.info("UDP connection lost")

    async def start(self):
        loop = asyncio.get_event_loop()
        transport, protocol = await loop.create_datagram_endpoint(lambda: self, local_addr=('127.0.0.1', 0), family=socket.AF_INET)
        self.transport = transport
        await self.request_mission()
        asyncio.create_task(self.retransmit_loop())

    async def request_mission(self):
        self.seq = (self.seq + 1) & 0xFFFFFFFF
        msgid = random.getrandbits(48)
        tlvs = []
        caps = ",".join(["sampling", "imaging", "env"])
        tlvs.append((binary_proto.TLV_CAPABILITIES, caps.encode("utf-8")))
        pkt = binary_proto.pack_ml_datagram(binary_proto.ML_REQUEST_MISSION, self.rover_id, tlvs, flags=binary_proto.FLAG_ACK_REQUESTED, seqnum=self.seq, msgid=msgid)
        if len(pkt) > getattr(config, "ML_MAX_DATAGRAM_SIZE", 1200):
            logger.error("Request mission packet too large (%d bytes) - not sent", len(pkt))
            return
        with self._lock:
            self.pending[int(msgid)] = (pkt, time.time(), 1)
        try:
            self.transport.sendto(pkt, self.server)
            logger.info("Sent REQUEST_MISSION msgid=%s to %s", msgid, self.server)
        except Exception:
            logger.exception("Failed to send REQUEST_MISSION")

    async def _progress_loop(self, mission_id: str):
        progress = 0.0
        while progress < 100.0:
            await asyncio.sleep(2.0)
            progress = min(100.0, progress + random.uniform(10, 40))
            tlvs = []
            tlvs.append((binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")))
            tlvs.append(binary_proto.tlv_progress(progress))
            tlvs.append(binary_proto.tlv_position(random.uniform(-5,5), random.uniform(-5,5), 0.0))
            tlvs.append(binary_proto.tlv_battery_level(random.randint(20,100)))
            msgid = random.getrandbits(48)
            pkt = binary_proto.pack_ml_datagram(binary_proto.ML_PROGRESS, self.rover_id, tlvs, flags=binary_proto.FLAG_ACK_REQUESTED, seqnum=self.seq, msgid=msgid)
            if len(pkt) > getattr(config, "ML_MAX_DATAGRAM_SIZE", 1200):
                logger.error("PROGRESS packet too large (%d bytes) - skipping", len(pkt))
                continue
            with self._lock:
                self.pending[int(msgid)] = (pkt, time.time(), 1)
            try:
                self.transport.sendto(pkt, self.server)
                logger.info("Sent PROGRESS %s pct=%.1f", mission_id, progress)
            except Exception:
                logger.exception("Failed to send PROGRESS")
        # send complete
        tlvs = [(binary_proto.TLV_MISSION_ID, mission_id.encode("utf-8")), (binary_proto.TLV_PARAMS_JSON, b'{"result":"success"}')]
        msgid = random.getrandbits(48)
        pkt = binary_proto.pack_ml_datagram(binary_proto.ML_MISSION_COMPLETE, self.rover_id, tlvs, flags=binary_proto.FLAG_ACK_REQUESTED, seqnum=self.seq, msgid=msgid)
        with self._lock:
            self.pending[int(msgid)] = (pkt, time.time(), 1)
        try:
            self.transport.sendto(pkt, self.server)
            logger.info("Sent MISSION_COMPLETE %s", mission_id)
        except Exception:
            logger.exception("Failed to send MISSION_COMPLETE")

    def _resend_persisted_packets(self, addr: Tuple[str, int]):
        """
        Read persisted packets from ML_CLIENT_CACHE_DIR and resend them to addr.
        Filename convention: "<rover_id>-<msgid>.bin"
        Records mapping msgid -> Path so file can be removed when ACK arrives.
        """
        if not self.cache_dir:
            logger.debug("No ML_CLIENT_CACHE_DIR configured; nothing to resend")
            return
        try:
            pattern = f"{self.rover_id}-*.bin"
            for p in sorted(self.cache_dir.glob(pattern)):
                try:
                    name = p.name
                    # Ensure filename actually starts with "<rover_id>-"
                    prefix = f"{self.rover_id}-"
                    if not name.startswith(prefix):
                        logger.debug("Skipping file with unexpected prefix: %s", name)
                        continue
                    # Extract the trailing part after the prefix and remove extension: "<msgid>.bin"
                    trailing = name[len(prefix):]
                    msgid_part = trailing.rsplit(".", 1)[0]
                    try:
                        msgid = int(msgid_part)
                    except Exception:
                        logger.debug("Skipping file with non-numeric msgid part: %s", name)
                        continue
                    data = p.read_bytes()
                    # send
                    if self.transport:
                        try:
                            self.transport.sendto(data, addr)
                            logger.info("Resent persisted packet %s to %s", p, addr)
                        except Exception:
                            logger.exception("Failed to send persisted packet %s", p)
                    else:
                        logger.debug("No transport available to resend %s", p)
                    # register in pending map so retransmit logic can track it
                    with self._lock:
                        self.pending[int(msgid)] = (data, time.time(), 1)
                        self._persisted_files[int(msgid)] = p
                except Exception:
                    logger.exception("Error resending persisted packet %s", p)
        except Exception:
            logger.exception("Error enumerating persisted packets in %s", self.cache_dir)

    async def retransmit_loop(self):
        while True:
            now = time.time()
            remove = []
            with self._lock:
                items = list(self.pending.items())
            for mid, (pkt, created, attempts) in items:
                timeout = config.TIMEOUT_TX_INITIAL * (config.BACKOFF_FACTOR ** (attempts - 1))
                if now - created > timeout:
                    if attempts <= config.N_RETX:
                        try:
                            self.transport.sendto(pkt, self.server)
                            with self._lock:
                                self.pending[mid] = (pkt, now, attempts + 1)
                            logger.warning("Retransmit pending msg %s attempt=%d", mid, attempts + 1)
                        except Exception:
                            logger.exception("Failed retransmit")
                    else:
                        logger.error("Retries exhausted for pending msg %s", mid)
                        remove.append(mid)
            with self._lock:
                for r in remove:
                    self.pending.pop(r, None)
                    # if file persisted for this msgid, try to remove mapping (but don't delete here)
                    self._persisted_files.pop(r, None)
            await asyncio.sleep(0.5)


# CLI entrypoint omitted for brevity; file still runnable as module if needed
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rover-id", required=True)
    parser.add_argument("--server", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=config.ML_UDP_PORT)
    parser.add_argument("--exit-on-complete", action="store_true")
    args = parser.parse_args()
    client = SimpleMLClient(args.rover_id, (args.server, args.port), exit_on_complete=args.exit_on_complete)
    try:
        asyncio.run(client.start())
    except KeyboardInterrupt:
        logger.info("Client shutting down")